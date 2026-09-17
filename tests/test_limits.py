"""The limiter's arithmetic, with no server and no Redis in the way."""

from __future__ import annotations

import pytest

from quotagate.limits import InMemoryBuckets, Limit, Scope, estimate_cost

KEY = Limit(requests_per_minute=10, tokens_per_minute=10_000)
ACCOUNT = Limit(requests_per_minute=30, tokens_per_minute=8_000)


def scopes(key: Limit = KEY, account: Limit | None = None) -> list[Scope]:
    result = [Scope("key:demo", key)]
    if account is not None:
        result.append(Scope("account", account))
    return result


@pytest.mark.asyncio
async def test_a_burst_is_admitted_up_to_the_limit_and_no_further() -> None:
    buckets = InMemoryBuckets()
    admitted = 0
    for _ in range(25):
        decision = await buckets.take(scopes(), cost=1)
        admitted += decision.allowed
    assert admitted == 10


@pytest.mark.asyncio
async def test_tokens_bind_before_requests_when_calls_are_expensive() -> None:
    """Groq's free tier binds on tokens first; the limiter has to agree."""
    buckets = InMemoryBuckets()
    limit = Limit(requests_per_minute=20, tokens_per_minute=6_000)
    admitted = 0
    for _ in range(20):
        decision = await buckets.take([Scope("key:demo", limit)], cost=3_000)
        admitted += decision.allowed
    assert admitted == 2  # 6,000 tokens / 3,000 per call, not 20 requests


@pytest.mark.asyncio
async def test_a_refusal_says_which_ceiling_bound() -> None:
    buckets = InMemoryBuckets()
    limit = Limit(requests_per_minute=1, tokens_per_minute=10_000)
    await buckets.take([Scope("key:demo", limit)], cost=1)
    decision = await buckets.take([Scope("key:demo", limit)], cost=1)
    assert not decision.allowed
    assert decision.bound_by == "key:demo:requests"
    assert decision.retry_after > 0


@pytest.mark.asyncio
async def test_a_refused_call_is_not_charged_for_tokens() -> None:
    buckets = InMemoryBuckets()
    limit = Limit(requests_per_minute=1, tokens_per_minute=10_000)
    await buckets.take([Scope("key:demo", limit)], cost=100)
    refused = await buckets.take([Scope("key:demo", limit)], cost=100)
    assert not refused.allowed
    assert refused.tokens_remaining == pytest.approx(9_900, abs=1)


@pytest.mark.asyncio
async def test_the_account_budget_refusing_does_not_charge_the_caller() -> None:
    """The reason both scopes are decided in one operation."""
    buckets = InMemoryBuckets()
    account = Limit(requests_per_minute=1, tokens_per_minute=8_000)

    first = await buckets.take(scopes(account=account), cost=10)
    refused = await buckets.take(scopes(account=account), cost=10)

    assert first.allowed
    assert not refused.allowed
    assert refused.bound_by == "account:requests"

    # The caller spent one request, not two: the refused call never ran.
    after = await buckets.take(scopes(), cost=10)
    assert after.requests_remaining == pytest.approx(8, abs=0.1)


@pytest.mark.asyncio
async def test_reconcile_refunds_an_over_estimate_and_charges_an_under_estimate() -> None:
    buckets = InMemoryBuckets()
    limit = Limit(requests_per_minute=100, tokens_per_minute=10_000)
    reserved = 3_000

    await buckets.take([Scope("key:demo", limit)], cost=reserved)
    await buckets.reconcile([Scope("key:demo", limit)], reserved=reserved, actual=500)
    refunded = await buckets.take([Scope("key:demo", limit)], cost=1)
    assert refunded.tokens_remaining == pytest.approx(9_499, abs=2)

    await buckets.reconcile([Scope("key:demo", limit)], reserved=100, actual=2_100)
    charged = await buckets.take([Scope("key:demo", limit)], cost=1)
    assert charged.tokens_remaining == pytest.approx(7_497, abs=3)


@pytest.mark.asyncio
async def test_refunds_cannot_mint_budget_above_the_ceiling() -> None:
    buckets = InMemoryBuckets()
    limit = Limit(requests_per_minute=100, tokens_per_minute=1_000)
    await buckets.take([Scope("key:demo", limit)], cost=10)
    for _ in range(20):
        await buckets.reconcile([Scope("key:demo", limit)], reserved=500, actual=0)
    decision = await buckets.take([Scope("key:demo", limit)], cost=1)
    assert decision.tokens_remaining <= 1_000


@pytest.mark.asyncio
async def test_three_copies_each_admit_the_whole_limit() -> None:
    """The baseline this project exists to disprove, stated as a test.

    Three in-memory limiters are three copies of the service. Each one admits
    the full quota, so the deployment admits three times what it promised.
    """
    copies = [InMemoryBuckets() for _ in range(3)]
    admitted = 0
    for index in range(30):
        decision = await copies[index % 3].take(scopes(), cost=1)
        admitted += decision.allowed
    assert admitted == 30  # the limit is 10


def test_estimate_reserves_the_completion_the_caller_allowed() -> None:
    payload = {"messages": [{"role": "user", "content": "x" * 400}], "max_tokens": 1_000}
    assert estimate_cost(payload) == 100 + 1_000


def test_estimate_handles_multi_part_content_and_missing_allowance() -> None:
    payload = {
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": "y" * 800}, {"type": "image"}]}
        ]
    }
    assert estimate_cost(payload) == 200 + 300  # default allowance


def test_estimate_survives_a_nonsense_allowance() -> None:
    payload = {"messages": [{"role": "user", "content": "hi"}], "max_tokens": "lots"}
    assert estimate_cost(payload) == max(1, 0 + 300)


@pytest.mark.asyncio
async def test_headers_never_invite_an_immediate_retry() -> None:
    buckets = InMemoryBuckets()
    limit = Limit(requests_per_minute=1, tokens_per_minute=10_000)
    await buckets.take([Scope("key:demo", limit)], cost=1)
    decision = await buckets.take([Scope("key:demo", limit)], cost=1)
    headers = decision.headers(limit)
    assert int(headers["retry-after"]) >= 1
    assert headers["ratelimit-limit"] == "1"
    assert headers["ratelimit-bound-by"] == "key:demo:requests"
