"""quotagate HTTP surface.

The contract is OpenAI's, because that is the one every SDK already speaks:
point a client's `base_url` here, keep the rest of the code, and the gateway's
limits, failover and accounting apply without anyone writing an adapter.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, Response, StreamingResponse

from quotagate import __version__, upstream
from quotagate.page import INDEX_HTML
from quotagate.config import limits as limit_settings
from quotagate.config import settings
from quotagate.keys import ApiKey, bearer_token, find, parse_keys
from quotagate.limits import Buckets, InMemoryBuckets, Limit, Scope, estimate_cost
from quotagate.redis_buckets import RedisBuckets, RestTransport, TcpTransport
from quotagate.requestlog import RequestRecord
from quotagate.upstream import UpstreamUnavailable

# A proxy that buffers would make the probe lie, so say no to buffering the
# only way HTTP lets you: Nginx and friends honour x-accel-buffering.
STREAM_HEADERS = {
    "cache-control": "no-cache, no-transform",
    "x-accel-buffering": "no",
}

MAX_CHUNKS = 200
MAX_GAP_MS = 2_000
# Big enough for a long conversation, small enough that an unauthenticated
# caller cannot make the gateway hold megabytes in memory.
MAX_BODY_BYTES = 256 * 1024


KEY_LIMIT = Limit(limit_settings.key_rpm, limit_settings.key_tpm)
ACCOUNT_LIMIT = Limit(limit_settings.account_rpm, limit_settings.account_tpm)


def build_buckets() -> Buckets:
    """Shared buckets when Redis is configured, per-process ones when not.

    The in-memory fallback is not a degraded mode of the same guarantee — it is
    a different, weaker one, so `/healthz` reports which is in force rather than
    letting a missing environment variable quietly halve the limits' meaning.
    """
    if limit_settings.redis_rest_url and limit_settings.redis_rest_token:
        return RedisBuckets(
            RestTransport(limit_settings.redis_rest_url, limit_settings.redis_rest_token)
        )
    if limit_settings.redis_url:
        return RedisBuckets(TcpTransport(limit_settings.redis_url))
    return InMemoryBuckets()


def scopes_for(key: ApiKey) -> list[Scope]:
    # The caller's own ceiling, then the provider account everybody shares.
    return [Scope(f"key:{key.name}", KEY_LIMIT), Scope("account", ACCOUNT_LIMIT)]


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    app.state.upstream = upstream.build_client(settings)
    app.state.keys = parse_keys(settings.keys_raw)
    app.state.buckets = build_buckets()
    try:
        yield
    finally:
        await app.state.upstream.aclose()
        await app.state.buckets.aclose()


app = FastAPI(title="quotagate", version=__version__, docs_url="/docs", lifespan=lifespan)


def error(status: int, code: str, message: str, headers: dict | None = None) -> JSONResponse:
    """OpenAI-shaped errors, so SDK error handling keeps working."""
    return JSONResponse(
        {"error": {"message": message, "type": code, "code": code}},
        status_code=status,
        headers=headers or {},
    )


def authenticate(request: Request) -> ApiKey | JSONResponse:
    keys: list[ApiKey] = request.app.state.keys
    if not keys:
        # Refusing is the safe failure. An open gateway in front of a real
        # provider key is worse than an unavailable one.
        return error(503, "not_configured", "no API keys are configured on this gateway")
    token = bearer_token(request.headers.get("authorization"))
    if token is None:
        return error(401, "missing_api_key", "provide a key as `Authorization: Bearer <key>`")
    key = find(token, keys)
    if key is None:
        return error(401, "invalid_api_key", "that key is not valid on this gateway")
    return key


@app.get("/", include_in_schema=False)
async def index() -> HTMLResponse:
    return HTMLResponse(INDEX_HTML)


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse(
        {
            "status": "ok",
            "version": __version__,
            "upstream": settings.upstream_name,
            "upstream_key_configured": settings.has_upstream_key,
            "keys_configured": len(app.state.keys) if hasattr(app.state, "keys") else 0,
            "limits": {
                # "per-process" is a weaker promise than "shared", so it is
                # reported rather than hidden behind the same word.
                "scope": "shared" if limit_settings.shared else "per-process",
                "key_rpm": KEY_LIMIT.requests_per_minute,
                "key_tpm": KEY_LIMIT.tokens_per_minute,
                "account_rpm": ACCOUNT_LIMIT.requests_per_minute,
                "account_tpm": ACCOUNT_LIMIT.tokens_per_minute,
                "on_limiter_failure": "allow" if limit_settings.fail_open else "refuse",
            },
        }
    )


@app.post("/v1/chat/completions")
async def chat_completions(request: Request) -> Response:
    key = authenticate(request)
    if isinstance(key, JSONResponse):
        return key

    raw = await request.body()
    if len(raw) > MAX_BODY_BYTES:
        return error(413, "payload_too_large", f"request body exceeds {MAX_BODY_BYTES} bytes")
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError:
        return error(400, "invalid_json", "request body is not valid JSON")
    if not isinstance(payload, dict) or not payload.get("model") or not payload.get("messages"):
        return error(400, "invalid_request", "`model` and `messages` are required")

    scopes = scopes_for(key)
    buckets: Buckets = request.app.state.buckets
    reserved = estimate_cost(payload)
    try:
        decision = await buckets.take(scopes, reserved)
    except Exception as exc:  # Redis unreachable, timing out, or refusing
        if not limit_settings.fail_open:
            # Refusing costs this caller a request. Allowing costs the provider
            # budget that every caller shares, and that is the worse trade.
            return error(
                503,
                "limiter_unavailable",
                f"rate limiter is unreachable ({type(exc).__name__}); request refused",
                {"retry-after": "5"},
            )
        decision = None

    if decision is not None and not decision.allowed:
        return error(
            429,
            "rate_limit_exceeded",
            f"rate limit reached for {decision.bound_by}",
            decision.headers(KEY_LIMIT),
        )

    wants_stream = bool(payload.get("stream"))
    if wants_stream and "stream_options" not in payload:
        # Without this the provider sends no usage frame, and every streamed
        # call would settle against an estimate instead of the real cost. The
        # caller sees one extra final frame, which OpenAI's own clients expect.
        payload["stream_options"] = {"include_usage": True}

    async def settle(completed: RequestRecord) -> None:
        if decision is None or not decision.reserved:
            return
        actual = (completed.prompt_tokens or 0) + (completed.completion_tokens or 0)
        if actual:
            await buckets.reconcile(scopes, decision.reserved, actual)

    record = RequestRecord(
        key_name=key.name,
        model=str(payload.get("model")),
        streamed=wants_stream,
        upstream=settings.upstream_name,
    )
    started = time.perf_counter()

    try:
        response = await upstream.open_stream(request.app.state.upstream, settings, payload)
    except UpstreamUnavailable as exc:
        record.finish(exc.status, "upstream_unreachable", started)
        record.emit()
        return error(exc.status, "upstream_unavailable", exc.reason)

    if response.status_code >= 400:
        # Nothing has been sent to the caller yet, which is the only point
        # where switching providers (v2) would still be honest.
        body = await response.aread()
        await response.aclose()
        record.finish(response.status_code, "upstream_error", started)
        record.emit()
        headers = {}
        if "retry-after" in response.headers:
            headers["retry-after"] = response.headers["retry-after"]
        try:
            return JSONResponse(json.loads(body), status_code=response.status_code, headers=headers)
        except json.JSONDecodeError:
            return error(
                response.status_code,
                "upstream_error",
                body.decode(errors="replace")[:500],
                headers,
            )

    limit_headers = decision.headers(KEY_LIMIT) if decision is not None else {}

    if wants_stream:
        return StreamingResponse(
            upstream.relay(response, record, started, on_finish=settle),
            media_type="text/event-stream",
            headers={**STREAM_HEADERS, **limit_headers},
        )

    body = await response.aread()
    await response.aclose()
    record.bytes_out = len(body)
    record.first_byte_ms = round((time.perf_counter() - started) * 1000, 1)
    try:
        parsed = json.loads(body)
    except json.JSONDecodeError:
        record.finish(502, "upstream_malformed", started)
        record.emit()
        return error(502, "upstream_malformed", "provider returned a body that is not JSON")
    usage = parsed.get("usage") or {}
    record.prompt_tokens = usage.get("prompt_tokens")
    record.completion_tokens = usage.get("completion_tokens")
    record.finish(response.status_code, "ok", started)
    record.emit()
    await settle(record)
    return JSONResponse(parsed, status_code=response.status_code, headers=limit_headers)


@app.get("/v1/models")
async def models(request: Request) -> Response:
    key = authenticate(request)
    if isinstance(key, JSONResponse):
        return key
    client: httpx.AsyncClient = request.app.state.upstream
    try:
        response = await client.get("/models", headers={"authorization": f"Bearer {settings.upstream_api_key}"})
    except httpx.HTTPError:
        return error(502, "upstream_unavailable", f"cannot reach {settings.upstream_name}")
    try:
        return JSONResponse(response.json(), status_code=response.status_code)
    except ValueError:
        return error(502, "upstream_malformed", "provider returned a body that is not JSON")


@app.post("/debug/limit-check")
async def limit_check(request: Request, cost: int = Query(1, ge=1, le=100_000)) -> Response:
    """Spend budget without calling the provider.

    The limiter decision is the thing being measured, and running it through
    real completions would mean burning a free-tier quota to find out whether
    the quota is being enforced. This takes the same path — same scopes, same
    buckets, same headers — and stops before the provider call.
    """
    key = authenticate(request)
    if isinstance(key, JSONResponse):
        return key

    scopes = scopes_for(key)
    try:
        decision = await request.app.state.buckets.take(scopes, cost)
    except Exception as exc:
        return error(503, "limiter_unavailable", f"{type(exc).__name__}", {"retry-after": "5"})

    status = 200 if decision.allowed else 429
    return JSONResponse(
        {
            "allowed": decision.allowed,
            "bound_by": decision.bound_by,
            "reserved": decision.reserved,
            "shared": limit_settings.shared,
        },
        status_code=status,
        headers=decision.headers(KEY_LIMIT),
    )


async def _ticks(chunks: int, gap_ms: int) -> AsyncIterator[str]:
    """Emit `chunks` SSE events, `gap_ms` apart, each stamped when it was sent.

    The stamp is what makes buffering detectable: if the server stamps events
    200ms apart but the client sees them all at once, the gap was added by the
    platform, not by us.
    """
    started = time.perf_counter()
    for i in range(chunks):
        if i:
            await asyncio.sleep(gap_ms / 1000)
        sent_ms = round((time.perf_counter() - started) * 1000, 1)
        yield f"data: {json.dumps({'i': i, 'sent_ms': sent_ms})}\n\n"
    yield "data: [DONE]\n\n"


@app.get("/debug/stream")
async def debug_stream(
    chunks: int = Query(20, ge=1, le=MAX_CHUNKS),
    gap_ms: int = Query(200, ge=0, le=MAX_GAP_MS),
) -> StreamingResponse:
    """Server-sent events at a known cadence, for scripts/measure_stream.py."""
    return StreamingResponse(
        _ticks(chunks, gap_ms),
        media_type="text/event-stream",
        headers=STREAM_HEADERS,
    )
