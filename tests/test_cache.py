"""What may be cached, what may not, and what a hit costs."""

from __future__ import annotations

import asyncio
from collections.abc import Iterator

import httpx
import pytest

from quotagate.cache import Entry, InMemoryCache, is_cacheable, key_for
from quotagate.keys import digest
from tests.servers import running

DEMO_KEY = "qg_test_key"
AUTH = {"authorization": f"Bearer {DEMO_KEY}"}


def body(**extra) -> dict:
    return {
        "model": "fake",
        "messages": [{"role": "user", "content": "what is the capital of Australia"}],
        "temperature": 0,
        **extra,
    }


@pytest.fixture(scope="module")
def upstream_url() -> Iterator[str]:
    with running("tests.fake_upstream:app") as url:
        yield f"{url}/v1"


@pytest.fixture(scope="module")
def gateway(upstream_url: str) -> Iterator[str]:
    env = {
        "QUOTAGATE_UPSTREAM_URL": upstream_url,
        "QUOTAGATE_UPSTREAM_NAME": "fake",
        "QUOTAGATE_KEYS": f"tests:{digest(DEMO_KEY)}",
        "QUOTAGATE_KEY_RPM": "1000",
        "QUOTAGATE_KEY_TPM": "1000000",
        "QUOTAGATE_ACCOUNT_RPM": "1000",
        "QUOTAGATE_ACCOUNT_TPM": "1000000",
    }
    with running("app:app", env=env) as url:
        yield url


def post(gateway: str, payload: dict) -> httpx.Response:
    return httpx.post(f"{gateway}/v1/chat/completions", json=payload, headers=AUTH, timeout=20)


def test_only_a_request_that_pinned_its_sampling_is_cacheable() -> None:
    assert is_cacheable(body())
    assert not is_cacheable({"messages": [], "temperature": 0.7})
    assert not is_cacheable({"messages": []}), "no temperature means the provider's default"
    assert not is_cacheable(body(tools=[{"type": "function"}])), "a tool call can differ each time"
    assert not is_cacheable(body(n=3))


def test_the_key_covers_everything_that_changes_the_answer() -> None:
    assert key_for(body()) == key_for(body())
    assert key_for(body()) != key_for(body(model="other"))
    assert key_for(body()) != key_for(body(max_tokens=50))
    assert key_for(body()) != key_for(
        {**body(), "messages": [{"role": "user", "content": "something else"}]}
    )
    # Streaming does not change the answer, only its shape.
    assert key_for(body()) == key_for(body(stream=True))


def test_a_repeated_deterministic_question_is_answered_from_the_cache(gateway: str) -> None:
    first = post(gateway, body())
    second = post(gateway, body())
    assert first.headers["x-quotagate-cache"] == "miss"
    assert second.headers["x-quotagate-cache"] == "hit"
    assert first.json() == second.json()


def test_a_cache_hit_spends_no_token_budget(gateway: str) -> None:
    """The reservation is handed back: no provider call, no tokens burned."""
    payload = body(messages=[{"role": "user", "content": "budget question"}])
    post(gateway, payload)  # warm it
    after_miss = post(gateway, payload)
    after_hit = post(gateway, payload)

    assert after_miss.headers["x-quotagate-cache"] == "hit"
    remaining_first = int(after_miss.headers["ratelimit-tokens-remaining"])
    remaining_second = int(after_hit.headers["ratelimit-tokens-remaining"])
    # Refills make exact equality flaky; the point is that hits do not drain it.
    assert remaining_second >= remaining_first - 1


def test_a_sampled_request_is_never_served_from_the_cache(gateway: str) -> None:
    payload = {**body(), "temperature": 0.9}
    assert post(gateway, payload).headers["x-quotagate-cache"] == "skip"
    assert post(gateway, payload).headers["x-quotagate-cache"] == "skip"


def test_a_cached_stream_replays_as_a_stream(gateway: str) -> None:
    payload = body(stream=True, messages=[{"role": "user", "content": "stream please"}])
    first = post(gateway, payload)
    second = post(gateway, payload)

    assert first.headers["x-quotagate-cache"] == "miss"
    assert second.headers["x-quotagate-cache"] == "hit"
    assert second.headers["content-type"].startswith("text/event-stream")
    assert second.text.count("data: ") == first.text.count("data: ")
    assert second.text.endswith("data: [DONE]\n\n")


def test_a_buffered_answer_is_not_served_to_a_streaming_caller(gateway: str) -> None:
    """The two shapes are stored separately; handing back the wrong one would
    give an SDK a body it cannot parse."""
    payload = body(messages=[{"role": "user", "content": "shape matters"}])
    post(gateway, payload)
    streamed = post(gateway, {**payload, "stream": True})
    assert streamed.headers["x-quotagate-cache"] == "miss"
    assert streamed.headers["content-type"].startswith("text/event-stream")


@pytest.mark.asyncio
async def test_entries_expire() -> None:
    cache = InMemoryCache()
    await cache.set("k", Entry(body=b"{}", streamed=False, status=200), ttl=0)
    await asyncio.sleep(0.01)
    assert await cache.get("k") is None


@pytest.mark.asyncio
async def test_the_cache_is_bounded_and_evicts_the_least_recently_used() -> None:
    cache = InMemoryCache(max_entries=2)
    for name in ("a", "b"):
        await cache.set(name, Entry(body=b"{}", streamed=False, status=200), ttl=60)
    await cache.get("a")  # a is now the most recently used
    await cache.set("c", Entry(body=b"{}", streamed=False, status=200), ttl=60)

    assert await cache.get("b") is None
    assert await cache.get("a") is not None
    assert await cache.get("c") is not None
