from __future__ import annotations

import json
import time

import httpx
import pytest

from app import app


@pytest.fixture
def client() -> httpx.AsyncClient:
    transport = httpx.ASGITransport(app=app)
    return httpx.AsyncClient(transport=transport, base_url="http://test")


@pytest.mark.asyncio
async def test_healthz(client: httpx.AsyncClient) -> None:
    async with client:
        response = await client.get("/healthz")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


@pytest.mark.asyncio
async def test_stream_declares_itself_unbuffered(client: httpx.AsyncClient) -> None:
    """Headers that tell proxies not to hold the response back.

    Whether chunks *actually* arrive one at a time is not decidable in-process
    — httpx's ASGI transport hands back the whole body — so that check lives in
    tests/test_stream_live.py against a real socket.
    """
    async with client:
        async with client.stream("GET", "/debug/stream?chunks=2&gap_ms=0") as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            assert response.headers["x-accel-buffering"] == "no"
            assert "no-transform" in response.headers["cache-control"]


@pytest.mark.asyncio
async def test_stream_stamps_are_monotonic(client: httpx.AsyncClient) -> None:
    stamps: list[float] = []
    async with client:
        async with client.stream("GET", "/debug/stream?chunks=4&gap_ms=10") as response:
            async for line in response.aiter_lines():
                if line.startswith("data: ") and line != "data: [DONE]":
                    stamps.append(json.loads(line[6:])["sent_ms"])

    assert stamps == sorted(stamps)
    assert stamps[0] < 5  # the first event leaves immediately


@pytest.mark.asyncio
async def test_stream_rejects_absurd_parameters(client: httpx.AsyncClient) -> None:
    """A public demo endpoint cannot let a caller ask for a 10-minute stream."""
    async with client:
        too_many = await client.get("/debug/stream?chunks=100000&gap_ms=200")
        too_slow = await client.get("/debug/stream?chunks=5&gap_ms=600000")
    assert too_many.status_code == 422
    assert too_slow.status_code == 422


@pytest.mark.asyncio
async def test_front_page_renders_without_a_key(client: httpx.AsyncClient) -> None:
    """A visitor with no key should still land on something that works."""
    async with client:
        response = await client.get("/")
    assert response.status_code == 200
    assert "text/html" in response.headers["content-type"]
    assert "/debug/stream" in response.text
