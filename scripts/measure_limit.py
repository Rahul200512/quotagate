#!/usr/bin/env python3
"""Fire a burst at one or more copies and count what got through.

    # three local copies, one shared Redis
    python scripts/measure_limit.py --key qg_... --limit 10 \\
        --url http://127.0.0.1:8101 --url http://127.0.0.1:8102 --url http://127.0.0.1:8103

    # the deployment
    python scripts/measure_limit.py --key qg_... --limit 10 --url https://<app>.vercel.app

Requests are spread across the URLs given. With per-process buckets, N copies
admit roughly N times the limit; with shared buckets the total holds at the
limit however many copies answer. The over-admission line is the number this
project exists to move.

The burst goes to /debug/limit-check, which takes the same limiter path as a
real call and stops before the provider, so measuring enforcement does not
spend the quota being enforced.
"""

from __future__ import annotations

import argparse
import asyncio
import collections
import sys

import httpx


async def _one(client: httpx.AsyncClient, url: str, key: str, cost: int) -> tuple[str, int]:
    try:
        response = await client.post(
            f"{url.rstrip('/')}/debug/limit-check?cost={cost}",
            headers={"authorization": f"Bearer {key}"},
        )
        return url, response.status_code
    except httpx.HTTPError:
        return url, 0


async def run(urls: list[str], key: str, total: int, cost: int, limit: int, timeout: float) -> int:
    async with httpx.AsyncClient(timeout=timeout) as client:
        results = await asyncio.gather(
            *(_one(client, urls[i % len(urls)], key, cost) for i in range(total))
        )

    admitted = sum(1 for _, status in results if status == 200)
    refused = sum(1 for _, status in results if status == 429)
    failed = sum(1 for _, status in results if status not in (200, 429))
    per_url = collections.Counter(url for url, status in results if status == 200)

    print(f"copies             {len(urls)}")
    print(f"requests sent      {total} (cost {cost} tokens each)")
    print(f"admitted           {admitted}")
    print(f"refused (429)      {refused}")
    if failed:
        print(f"failed             {failed}")
    print(f"limit              {limit}")
    over = admitted - limit
    print(f"over-admitted      {over} ({over / limit * 100:.0f}% over the limit)")
    for url in urls:
        print(f"  admitted by {url}   {per_url.get(url, 0)}")
    return 0 if over <= 0 else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", action="append", required=True, help="repeat for each copy")
    parser.add_argument("--key", required=True, help="a quotagate API key")
    parser.add_argument("--limit", type=int, required=True, help="the limit that should hold")
    parser.add_argument("--requests", type=int, default=60)
    parser.add_argument("--cost", type=int, default=1, help="tokens to reserve per request")
    parser.add_argument("--timeout", type=float, default=20.0)
    args = parser.parse_args()

    if args.requests > 500:
        print("refusing to send more than 500 requests from a laptop", file=sys.stderr)
        return 2
    return asyncio.run(run(args.url, args.key, args.requests, args.cost, args.limit, args.timeout))


if __name__ == "__main__":
    raise SystemExit(main())
