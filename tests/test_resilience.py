"""Breaker and budget mechanics, with time passed in rather than waited for."""

from __future__ import annotations

import pytest

from quotagate.resilience import CircuitBreaker, RetryBudget, State


def test_a_breaker_stays_closed_while_failures_are_occasional() -> None:
    """A 10% error rate is a bad day, not an outage."""
    breaker = CircuitBreaker(threshold=5)
    for _ in range(4):
        breaker.record_failure(now=0)
        breaker.record_success()
    assert breaker.state(now=0) is State.CLOSED
    assert breaker.allows(now=0)


def test_a_breaker_opens_after_consecutive_failures() -> None:
    breaker = CircuitBreaker(threshold=3, cooldown=10)
    for _ in range(3):
        breaker.record_failure(now=0)
    assert breaker.state(now=0) is State.OPEN
    assert not breaker.allows(now=5)


def test_one_call_is_let_through_after_the_cooldown() -> None:
    breaker = CircuitBreaker(threshold=3, cooldown=10)
    for _ in range(3):
        breaker.record_failure(now=0)

    assert breaker.state(now=11) is State.HALF_OPEN
    assert breaker.allows(now=11), "the first caller after the cooldown finds out"
    assert not breaker.allows(now=11), "everyone else keeps failing fast until it reports back"


def test_a_successful_trial_closes_the_breaker() -> None:
    breaker = CircuitBreaker(threshold=2, cooldown=5)
    breaker.record_failure(now=0)
    breaker.record_failure(now=0)
    breaker.allows(now=6)
    breaker.record_success()
    assert breaker.state(now=6) is State.CLOSED


def test_a_failed_trial_restarts_the_whole_cooldown() -> None:
    """A provider that fails its trial has not recovered; give it the full rest."""
    breaker = CircuitBreaker(threshold=2, cooldown=5)
    breaker.record_failure(now=0)
    breaker.record_failure(now=0)
    breaker.allows(now=6)
    breaker.record_failure(now=6)
    assert breaker.state(now=8) is State.OPEN
    assert breaker.state(now=12) is State.HALF_OPEN


@pytest.mark.asyncio
async def test_the_budget_runs_out_when_everything_is_failing() -> None:
    budget = RetryBudget(ratio=0.2, burst=5, tokens=5)
    spent = 0
    for _ in range(20):
        budget.record_request()
        spent += await budget.try_spend()
    # 5 banked, then one retry per five requests: 8 over 20 calls, not 20.
    assert spent == 8


@pytest.mark.asyncio
async def test_the_budget_refills_from_traffic_that_does_not_retry() -> None:
    budget = RetryBudget(ratio=0.2, burst=5, tokens=0)
    for _ in range(5):
        budget.record_request()
    assert await budget.try_spend(), "five quiet requests pay for one retry"
    assert not await budget.try_spend()


@pytest.mark.asyncio
async def test_the_budget_cannot_bank_more_than_its_burst() -> None:
    budget = RetryBudget(ratio=0.5, burst=3, tokens=3)
    for _ in range(50):
        budget.record_request()
    spent = 0
    for _ in range(10):
        spent += await budget.try_spend()
    assert spent == 3
