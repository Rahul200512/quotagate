"""The streaming check that needs a real socket.

An in-process ASGI transport reports whatever the app yielded, all at once, so
it cannot tell a streaming server from a buffering one. This starts uvicorn on
a port and measures when bytes actually arrive. The same measurement runs
against the deployed URL via scripts/measure_stream.py.
"""

from __future__ import annotations

import socket
import subprocess
import sys
import time
from collections.abc import Iterator

import httpx
import pytest


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@pytest.fixture(scope="module")
def live_server() -> Iterator[str]:
    port = _free_port()
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app:app", "--port", str(port), "--log-level", "warning"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            if process.poll() is not None:
                raise RuntimeError("uvicorn exited before it was ready")
            try:
                if httpx.get(f"{base_url}/healthz", timeout=0.5).status_code == 200:
                    break
            except httpx.HTTPError:
                time.sleep(0.1)
        else:
            raise RuntimeError("uvicorn did not become ready in 20s")
        yield base_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()


def test_events_arrive_one_at_a_time(live_server: str) -> None:
    from scripts.measure_stream import measure

    assert measure(live_server, chunks=6, gap_ms=100, timeout=30) == 0


def test_client_disconnect_stops_the_generator(live_server: str) -> None:
    """Hanging up must not leave the server producing into nothing.

    v0 only proves the request ends. Cancelling the *upstream* model call on
    disconnect is v1's job, and is where the real cost saving is.
    """
    with httpx.Client(timeout=10) as client:
        with client.stream("GET", f"{live_server}/debug/stream?chunks=200&gap_ms=50") as response:
            for line in response.iter_lines():
                if line.startswith("data: "):
                    break  # walk away after the first event

    assert httpx.get(f"{live_server}/healthz", timeout=5).status_code == 200
