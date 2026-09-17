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

import time
from collections.abc import AsyncIterator

import httpx

from quotagate.config import Settings
from quotagate.requestlog import RequestRecord


class UpstreamUnavailable(Exception):
    """The provider could not be reached, or went silent mid-stream."""

    def __init__(self, reason: str, status: int) -> None:
        super().__init__(reason)
        self.reason = reason
        self.status = status


def build_client(settings: Settings) -> httpx.AsyncClient:
    return httpx.AsyncClient(
        base_url=settings.upstream_base_url,
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


def _headers(settings: Settings) -> dict[str, str]:
    headers = {"content-type": "application/json"}
    if settings.upstream_api_key:
        headers["authorization"] = f"Bearer {settings.upstream_api_key}"
    return headers


async def open_stream(
    client: httpx.AsyncClient,
    settings: Settings,
    payload: dict,
) -> httpx.Response:
    """Send the request and return the open response, status already known.

    Raises UpstreamUnavailable when the provider never answered. The caller
    owns the returned response and must close it.
    """
    request = client.build_request(
        "POST", "/chat/completions", json=payload, headers=_headers(settings)
    )
    try:
        return await client.send(request, stream=True)
    except (httpx.ConnectError, httpx.ConnectTimeout) as exc:
        raise UpstreamUnavailable(f"cannot reach {settings.upstream_name}", 502) from exc
    except httpx.ReadTimeout as exc:
        raise UpstreamUnavailable(f"{settings.upstream_name} timed out", 504) from exc
    except httpx.HTTPError as exc:  # pragma: no cover - defensive
        raise UpstreamUnavailable(f"{settings.upstream_name} failed: {exc!r}", 502) from exc


async def relay(
    response: httpx.Response,
    record: RequestRecord,
    started: float,
) -> AsyncIterator[bytes]:
    """Hand provider bytes to the caller as they arrive, then log.

    If the caller disconnects, Starlette closes this generator, `finally` runs,
    and closing the response tears down the upstream connection — so walking
    away actually stops the work instead of paying for tokens nobody reads.
    """
    outcome = "ok"
    try:
        async for chunk in response.aiter_raw():
            if not chunk:
                continue
            if record.first_byte_ms is None:
                record.first_byte_ms = round((time.perf_counter() - started) * 1000, 1)
            record.chunks += 1
            record.bytes_out += len(chunk)
            yield chunk
    except GeneratorExit:
        outcome = "client_disconnect"
        raise
    except httpx.ReadTimeout:
        outcome = "upstream_timeout"
        raise
    finally:
        record.finish(response.status_code, outcome, started)
        await response.aclose()
        record.emit()
