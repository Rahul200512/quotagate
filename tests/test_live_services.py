"""Checks against the real Upstash and the real providers.

Skipped unless the credentials are in the environment, so CI stays offline and
free. Run them with:

    set -a && . ./.env.local && set +a && .venv/bin/python -m pytest tests/test_live_services.py -q

These are the tests that catch what a fake provider cannot: a Lua dialect
Upstash rejects, a model id that no longer exists, an auth header the provider
spells differently.
"""

from __future__ import annotations

import os
import time
import uuid

import httpx
import pytest

from quotagate.limits import Limit, Scope
from quotagate.redis_buckets import RedisBuckets, RestTransport

REDIS_URL = os.environ.get("QUOTAGATE_REDIS_REST_URL")
REDIS_TOKEN = os.environ.get("QUOTAGATE_REDIS_REST_TOKEN")
GROQ_KEY = os.environ.get("QUOTAGATE_PROVIDER_GROQ_KEY")
OPENROUTER_KEY = os.environ.get("QUOTAGATE_PROVIDER_OPENROUTER_KEY")

# Available on both providers, which is what makes failover between them
# possible at all — see the model-mapping caveat in the README.
SHARED_MODEL = "openai/gpt-oss-20b"

needs_redis = pytest.mark.skipif(
    not (REDIS_URL and REDIS_TOKEN), reason="Upstash credentials not in the environment"
)
needs_groq = pytest.mark.skipif(not GROQ_KEY, reason="Groq key not in the environment")
needs_openrouter = pytest.mark.skipif(
    not OPENROUTER_KEY, reason="OpenRouter key not in the environment"
)


def buckets() -> RedisBuckets:
    # A fresh namespace per run: buckets outlive a test, and a leftover bucket
    # would make the next run's first call look rate limited.
    return RedisBuckets(RestTransport(REDIS_URL, REDIS_TOKEN), namespace=f"t{uuid.uuid4().hex[:8]}")


@needs_redis
@pytest.mark.asyncio
async def test_the_lua_script_runs_on_upstash() -> None:
    """Upstash runs Lua through its HTTP API; dialects differ, so ask it."""
    store = buckets()
    scopes = [Scope("key:test", Limit(3, 10_000)), Scope("account", Limit(100, 100_000))]
    try:
        decisions = [await store.take(scopes, 10) for _ in range(5)]
    finally:
        await store.aclose()

    assert [d.allowed for d in decisions] == [True, True, True, False, False]
    assert decisions[-1].bound_by == "key:test:requests"
    assert decisions[-1].retry_after > 0


@needs_redis
@pytest.mark.asyncio
async def test_reconciling_an_over_estimate_returns_tokens_on_upstash() -> None:
    store = buckets()
    scopes = [Scope("key:test", Limit(100, 10_000))]
    try:
        await store.take(scopes, 1_000)
        after_reserve = (await store.take(scopes, 1)).tokens_remaining
        await store.reconcile(scopes, reserved=1_000, actual=100)
        after_refund = (await store.take(scopes, 1)).tokens_remaining
    finally:
        await store.aclose()

    assert after_refund > after_reserve + 800


@needs_redis
@pytest.mark.asyncio
async def test_two_holders_of_one_bucket_cannot_both_spend_it() -> None:
    """Two RedisBuckets sharing a namespace are two copies of the service."""
    namespace = f"t{uuid.uuid4().hex[:8]}"
    copies = [
        RedisBuckets(RestTransport(REDIS_URL, REDIS_TOKEN), namespace=namespace) for _ in range(2)
    ]
    scopes = [Scope("key:test", Limit(6, 10_000))]
    try:
        admitted = 0
        for index in range(12):
            decision = await copies[index % 2].take(scopes, 1)
            admitted += decision.allowed
    finally:
        for copy in copies:
            await copy.aclose()

    assert admitted == 6, "the limit belongs to the key, not to a process"


@needs_groq
def test_groq_still_serves_the_model_the_readme_names() -> None:
    """Model ids are retired without warning; the README should not outlive one."""
    response = httpx.get(
        "https://api.groq.com/openai/v1/models",
        headers={"authorization": f"Bearer {GROQ_KEY}"},
        timeout=20,
    )
    assert response.status_code == 200
    assert SHARED_MODEL in {model["id"] for model in response.json()["data"]}


@needs_openrouter
def test_openrouter_serves_the_same_model() -> None:
    response = httpx.get("https://openrouter.ai/api/v1/models", timeout=30)
    assert response.status_code == 200
    assert SHARED_MODEL in {model["id"] for model in response.json()["data"]}


@needs_groq
def test_a_real_streamed_completion_arrives_in_pieces() -> None:
    """The provider's own stream, timed — not the probe's synthetic one.

    Groq is fast enough that a 30-token reply can land inside a single network
    read, which made the first version of this test fail against a provider
    that was streaming correctly. Asking for a longer reply makes the spread
    real rather than an artifact of how quickly it finished.
    """
    arrivals: list[float] = []
    started = time.perf_counter()
    with httpx.Client(timeout=60) as client:
        with client.stream(
            "POST",
            "https://api.groq.com/openai/v1/chat/completions",
            headers={"authorization": f"Bearer {GROQ_KEY}"},
            json={
                "model": SHARED_MODEL,
                "stream": True,
                "max_tokens": 250,
                "messages": [
                    {"role": "user", "content": "Write a short paragraph about rate limiting."}
                ],
            },
        ) as response:
            assert response.status_code == 200
            for line in response.iter_lines():
                if line.startswith("data: ") and "[DONE]" not in line:
                    arrivals.append((time.perf_counter() - started) * 1000)

    assert len(arrivals) > 20, "a reply this long should arrive as many frames"
    assert arrivals[-1] - arrivals[0] > 10, "a whole reply in one instant means it was buffered"


@needs_redis
@pytest.mark.asyncio
async def test_a_cached_reply_crosses_from_one_copy_to_another() -> None:
    """The point of a shared cache: instance A's answer serves instance B.

    On Vercel, consecutive requests routinely land on different instances, so
    a per-process cache is closer to a coincidence than a cache.
    """
    from quotagate.cache import Entry, RedisCache

    namespace = f"t{uuid.uuid4().hex[:8]}"
    writer = RedisCache(RestTransport(REDIS_URL, REDIS_TOKEN), namespace=namespace)
    reader = RedisCache(RestTransport(REDIS_URL, REDIS_TOKEN), namespace=namespace)

    body = b'data: {"choices":[{"delta":{"content":"hi"}}]}\n\ndata: [DONE]\n\n'
    await writer.set("shared-key", Entry(body=body, streamed=True, status=200), ttl=60)
    found = await reader.get("shared-key")
    missing = await reader.get("never-written")

    assert found is not None
    assert found.body == body, "the bytes must survive the round trip exactly"
    assert found.streamed is True
    assert missing is None


@needs_redis
@pytest.mark.asyncio
async def test_the_shared_retry_budget_runs_out_for_everyone() -> None:
    """Two copies draw retries from one allowance, not one each."""
    from quotagate.resilience import SharedRetryBudget

    namespace = f"t{uuid.uuid4().hex[:8]}"
    copies = [
        SharedRetryBudget(
            RestTransport(REDIS_URL, REDIS_TOKEN),
            retries_per_minute=1,
            burst=3,
            namespace=namespace,
        )
        for _ in range(2)
    ]

    spent = 0
    for index in range(8):
        spent += await copies[index % 2].try_spend()

    assert spent == 3, "the burst is the deployment's, not each copy's"


@needs_redis
@pytest.mark.asyncio
async def test_a_retry_is_refused_when_the_budget_store_is_unreachable() -> None:
    """Failing to reach the limiter must not turn one failing call into two."""
    from quotagate.resilience import SharedRetryBudget

    budget = SharedRetryBudget(RestTransport("http://127.0.0.1:1", "x"), retries_per_minute=60)
    assert await budget.try_spend() is False
