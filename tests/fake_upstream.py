"""A provider that misbehaves on demand.

Every failure this gateway claims to survive has to be reproducible on a
laptop, offline, without spending a token of anyone's quota. The behaviour is
chosen by model name, so a test asks for it the same way a caller asks for a
model.

    fake                normal streaming or buffered completion
    fake-429            rate limited, with retry-after
    fake-500            provider error with a non-JSON body
    fake-die            two chunks, then the connection drops mid-stream
    fake-slow           first chunk arrives after a long pause
    fake-400            the caller's request is wrong, not the provider's fault

Prefixing a mode with a provider name ("alpha:fake-500") aims it at that
provider only; every other provider treats the request as normal. Without that,
two fakes standing in for two providers would fail in lockstep and no failover
could ever be observed.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from collections.abc import AsyncIterator

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse

NAME = os.environ.get("FAKE_NAME", "fake")

app = FastAPI(title="fake-upstream")

CHUNK_GAP_S = 0.05
CHUNKS = 5
WORDS = ["Rate", " limits", " belong", " to", " the", " account", "."]


def _chunk(index: int, content: str) -> str:
    frame = {
        "id": "chatcmpl-fake",
        "object": "chat.completion.chunk",
        "created": int(time.time()),
        "model": "fake",
        "choices": [{"index": 0, "delta": {"content": content}, "finish_reason": None}],
    }
    return f"data: {json.dumps(frame)}\n\n"


async def _stream(model: str) -> AsyncIterator[str]:
    for i in range(CHUNKS):
        if model == "fake-die" and i == 2:
            raise RuntimeError("upstream died mid-stream")
        if i:
            await asyncio.sleep(CHUNK_GAP_S)
        yield _chunk(i, WORDS[i % len(WORDS)])
    yield (
        'data: {"id":"chatcmpl-fake","object":"chat.completion.chunk",'
        '"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],'
        '"usage":{"prompt_tokens":11,"completion_tokens":7,"total_tokens":18}}\n\n'
    )
    yield "data: [DONE]\n\n"


@app.post("/v1/chat/completions")
async def chat_completions(request: Request) -> Response:
    payload = await request.json()
    requested = str(payload.get("model", "fake"))
    target, separator, mode = requested.partition(":")
    model = (mode if target == NAME else "fake") if separator else requested

    if model == "fake-429":
        return JSONResponse(
            {"error": {"message": "rate limit reached", "type": "rate_limit_exceeded"}},
            status_code=429,
            headers={"retry-after": "3"},
        )
    if model == "fake-400":
        return JSONResponse(
            {"error": {"message": "unknown parameter", "type": "invalid_request_error"}},
            status_code=400,
        )
    if model == "fake-500":
        return Response("upstream exploded", status_code=500, media_type="text/plain")
    if model == "fake-slow":
        await asyncio.sleep(2.0)

    if payload.get("stream"):
        return StreamingResponse(_stream(model), media_type="text/event-stream")

    return JSONResponse(
        {
            "id": "chatcmpl-fake",
            "object": "chat.completion",
            "model": model,
            "answered_by": NAME,
            "choices": [
                {
                    "index": 0,
                    "message": {"role": "assistant", "content": "".join(WORDS[:CHUNKS])},
                    "finish_reason": "stop",
                }
            ],
            "usage": {"prompt_tokens": 11, "completion_tokens": 7, "total_tokens": 18},
        }
    )


@app.get("/v1/models")
async def models() -> JSONResponse:
    return JSONResponse({"object": "list", "data": [{"id": "fake", "object": "model"}]})
