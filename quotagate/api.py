"""quotagate HTTP surface.

v0 carries two endpoints only: a health check, and the streaming probe that
answers the question the whole hosting decision rests on — does a Python
function on Vercel deliver chunks as they are produced, or does it buffer the
whole response and send it at the end?
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import AsyncIterator

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse, StreamingResponse

from quotagate import __version__

app = FastAPI(title="quotagate", version=__version__, docs_url="/docs")

# A proxy that buffers would make the probe lie, so say no to buffering the
# only way HTTP lets you: Nginx and friends honour x-accel-buffering.
STREAM_HEADERS = {
    "cache-control": "no-cache, no-transform",
    "x-accel-buffering": "no",
}

MAX_CHUNKS = 200
MAX_GAP_MS = 2_000


@app.get("/healthz")
async def healthz() -> JSONResponse:
    return JSONResponse({"status": "ok", "version": __version__})


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
