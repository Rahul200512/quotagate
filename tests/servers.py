"""Start real servers on real ports for tests.

In-process ASGI transports hide exactly the behaviour this project is about:
when bytes arrive, and what happens when a connection is cut. So the proxy
tests talk to sockets.
"""

from __future__ import annotations

import os
import socket
import subprocess
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager

import httpx

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextmanager
def running(target: str, env: dict[str, str] | None = None) -> Iterator[str]:
    """Run `uvicorn <target>` until the block exits, yielding its base URL."""
    port = free_port()
    process = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", target, "--port", str(port), "--log-level", "warning"],
        cwd=REPO_ROOT,
        env={**os.environ, **(env or {})},
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    base_url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 20
        while True:
            if process.poll() is not None:
                raise RuntimeError(f"{target} exited before it was ready")
            if time.monotonic() > deadline:
                raise RuntimeError(f"{target} did not become ready in 20s")
            try:
                httpx.get(f"{base_url}/openapi.json", timeout=0.5)
                break
            except httpx.HTTPError:
                time.sleep(0.1)
        yield base_url
    finally:
        process.terminate()
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            process.kill()
