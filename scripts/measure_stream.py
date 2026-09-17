#!/usr/bin/env python3
"""Measure whether a quotagate deployment streams or buffers.

    python scripts/measure_stream.py http://127.0.0.1:8000
    python scripts/measure_stream.py https://<deployment>.vercel.app

Prints the arrival gap between server-sent events and a verdict. A server that
streams shows arrival gaps close to the cadence it was asked for; a server
behind a buffering proxy shows near-zero gaps after one long wait, because
every event was held back and released together.

Exit code is 0 when the response streamed, 1 when it was buffered.
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time

import httpx


def measure(base_url: str, chunks: int, gap_ms: int, timeout: float) -> int:
    url = f"{base_url.rstrip('/')}/debug/stream?chunks={chunks}&gap_ms={gap_ms}"
    arrivals: list[float] = []
    first_byte: float | None = None

    started = time.perf_counter()
    with httpx.Client(timeout=timeout) as client:
        with client.stream("GET", url) as response:
            response.raise_for_status()
            content_type = response.headers.get("content-type", "")
            for line in response.iter_lines():
                if not line.startswith("data: "):
                    continue
                now = (time.perf_counter() - started) * 1000
                if first_byte is None:
                    first_byte = now
                if line == "data: [DONE]":
                    break
                arrivals.append(now)

    if len(arrivals) < 2:
        print(f"only {len(arrivals)} events arrived; cannot judge", file=sys.stderr)
        return 1

    gaps = [b - a for a, b in zip(arrivals, arrivals[1:])]
    median_gap = statistics.median(gaps)
    # Buffered responses arrive in one burst, so the median gap collapses
    # toward zero. Half the requested cadence is a wide margin: a streaming
    # response over a slow network stays well above it.
    streamed = median_gap >= gap_ms * 0.5

    print(f"url                {url}")
    print(f"content-type       {content_type}")
    print(f"events             {len(arrivals)}")
    print(f"time to first event {first_byte:.0f} ms")
    print(f"gap requested      {gap_ms} ms")
    print(f"gap median         {median_gap:.0f} ms")
    print(f"gap min / max      {min(gaps):.0f} / {max(gaps):.0f} ms")
    print(f"total              {arrivals[-1]:.0f} ms")
    print(f"verdict            {'STREAMED' if streamed else 'BUFFERED'}")
    return 0 if streamed else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("base_url", help="e.g. http://127.0.0.1:8000")
    parser.add_argument("--chunks", type=int, default=20)
    parser.add_argument("--gap-ms", type=int, default=200)
    parser.add_argument("--timeout", type=float, default=60.0)
    args = parser.parse_args()
    return measure(args.base_url, args.chunks, args.gap_ms, args.timeout)


if __name__ == "__main__":
    raise SystemExit(main())
