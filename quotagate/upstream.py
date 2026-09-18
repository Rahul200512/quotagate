"""Talking to the model provider.

Two things here are deliberate and both matter later:

1. The request is opened with `stream=True` and the status is inspected before
   any byte is handed to the caller. Failing over to another provider is only
   honest before the first byte — once tokens have been sent, the caller has
   half an answer and a second provider cannot continue it. v2 hooks into
   exactly this gap.

2. The streaming body is passed through as raw bytes. Parsing and re-encoding
   each SSE frame would add latency to every token and give the gateway an
   opinion about a format the provider owns. Usage accounting in v1 reads the
   final frame instead of reshaping all of them.
"""

from __future__ import annotations

import json
import time
from collections.abc import AsyncIterator, Awaitable, Callable

import httpx

from quotagate.config import Settings
from quotagate.requestlog import RequestRecord


# Enough to hold the last few SSE frames, where usage lives; small enough that
# a long reply is never accumulated in memory.
TAIL_BYTES = 8 * 1024


class Capture:
    """Collect a streamed body for the cache, and give up if it grows too big.

    Giving up matters more than collecting: a cache is an optimisation, and an
    unbounded buffer in front of a streaming response would trade a latency win
    for a memory leak.
    """

    def __init__(self, limit: int) -> None:
        self._chunks: list[bytes] = []
        self._size = 0
        self._limit = limit
        self.abandoned = False

    def add(self, chunk: bytes) -> None:
        if self.abandoned:
            return
        self._size += len(chunk)
        if self._size > self._limit:
            self.abandoned = True
            self._chunks.clear()
            return
        self._chunks.append(chunk)

    @property
    def body(self) -> bytes | None:
        return None if self.abandoned else b"".join(self._chunks)


class UpstreamUnavailable(Exception):
    """The provider could not be reached, or went silent mid-stream."""

    def __init__(self, reason: str, status: int) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status = status


def build_client(settings: Settings, base_url: str | None = None) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=base_url or settings.upstream_base_url,
        timeout=httpx.Timeout(
            connect=settings.connect_timeout,
            read=settings.read_timeout,
            write=10.0,
            pool=5.0,
        ),
        # One pool for the process. Vercel keeps a function instance warm
        # between requests, so reusing connections skips a TLS handshake on
        # most calls.
        limits=httpx.Limits(max_connections=20, max_keepalive_connections=10),
    )


def _headers(settings: Settings, api_key: str | None = None) -> dict[str, str]:
    headers = {"content-type": "application/json"}
    key = api_key if api_key is not None else settings.upstream_api_key
    if key:
        headers["authorization"] = f"Bearer {key}"
    return headers


async def open_stream(
    client: httpx.AsyncClient,
    settings: Settings,
    payload: dict,
    api_key: str | None = None,
    provider_name: str | None = None,
) -> httpx.Response:
    """Send the request and return the open response, status already known.

    Raises UpstreamUnavailable when the provider never answered. The caller
    owns the returned response and must close it.
    """
    name = provider_name or settings.upstream_name
    request = client.build_request(
        "POST", "/chat/completions", json=payload, headers=_headers(settings, api_key)
    )
    try:
        return await client.send(request, stream=True)
    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
        raise UpstreamUnavailable(f"cannot reach {name}", 502) from exc
    except httpx.ReadTimeout as exc:
        raise UpstreamUnavailable(f"{name} timed out", 504) from exc
    except httpx.HTTPError as exc:  # pragma: no cover - defensive
        raise UpstreamUnavailable(f"{name} failed: {exc!r}", 502) from exc


def usage_from_tail(tail: bytes) -> dict | None:
    """Find the usage block a provider puts in its final streamed frames.

    Only the tail is kept — a few kilobytes — because holding the whole
    response to read its last frame would undo the point of streaming.
    """
    for line in reversed(tail.split(b"\n")):
        line = line.strip()
        if not line.startswith(b"data: ") or line == b"data: [DONE]":
            continue
        try:
            frame = json.loads(line[6:])
        except json.JSONDecodeError:
            continue  # a truncated first frame in the tail window
        usage = frame.get("usage") or (frame.get("x_groq") or {}).get("usage")
        if isinstance(usage, dict) and usage.get("total_tokens") is not None:
            return usage
    return None


async def relay(
    response: httpx.Response,
    record: RequestRecord,
    started: float,
    on_finish: Callable[[RequestRecord], Awaitable[None]] | None = None,
    capture: Capture | None = None,
) -> AsyncIterator[bytes]:
    """Hand provider bytes to the caller as they arrive, then log.

    If the caller disconnects, Starlette closes this generator, `finally` runs,
    and closing the response tears down the upstream connection — so walking
    away actually stops the work instead of paying for tokens nobody reads.
    """
    outcome = "ok"
    tail = b""
    try:
        async for chunk in response.aiter_raw():
            if not chunk:
                continue
            if record.first_byte_ms is None:
                record.first_byte_ms = round((time.perf_counter() - started) * 1000, 1)
            record.chunks += 1
            record.bytes_out += len(chunk)
            tail = (tail + chunk)[-TAIL_BYTES:]
            if capture is not None:
                capture.add(chunk)
            yield chunk
    except GeneratorExit:
        outcome = "client_disconnect"
        raise
    except httpx.ReadTimeout:
        outcome = "upstream_timeout"
        raise
    finally:
        usage = usage_from_tail(tail) if outcome == "ok" else None
        if usage:
            record.prompt_tokens = usage.get("prompt_tokens")
            record.completion_tokens = usage.get("completion_tokens")
        record.finish(response.status_code, outcome, started)
        await response.aclose()
        record.emit()
        # Settling the reservation is skipped on a disconnect: the generator is
        # being torn down and awaiting there is not reliable. The caller keeps
        # the full reservation until it refills, which errs toward under-
        # serving rather than over-spending the shared budget.
        if on_finish is not None and outcome == "ok":
            try:
                await on_finish(record)
            except Exception:  # pragma: no cover - accounting must not break a reply
                pass
