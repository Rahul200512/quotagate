"""Deciding when to stop asking a provider, and when to stop retrying at all.

Both mechanisms exist to protect the *provider* as much as the caller. A
gateway that retries every failed request turns a provider's bad minute into a
worse one: the moment it starts failing, its traffic doubles.

The circuit breaker answers "is it worth asking this provider right now".
The retry budget answers "can the system afford another attempt at all".
They are separate because they fail differently: a breaker protects one
provider, a budget protects the deployment's total load.

Both are per-process today. That is honest for a breaker — each copy learns
from what it has seen, and a provider that is down fails everyone's first
attempt quickly — but it makes the budget N times looser with N copies. Moving
both into Redis lands with v1's shared buckets, once that path is verified
against a real Redis.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from enum import Enum


class State(str, Enum):
    CLOSED = "closed"  # asking normally
    OPEN = "open"  # not asking; the provider gets a rest
    HALF_OPEN = "half_open"  # one trial call decides which way it goes


@dataclass
class CircuitBreaker:
    """Open after consecutive failures; let one call through after a cooldown.

    Consecutive failures, not a failure rate: a provider that fails five calls
    in a row is down, whereas a 10% error rate is a bad day, not an outage, and
    cutting traffic for it would cost more than it saves.
    """

    threshold: int = 5
    cooldown: float = 20.0
    failures: int = 0
    opened_at: float = 0.0
    trial_in_flight: bool = False

    def state(self, now: float | None = None) -> State:
        now = time.monotonic() if now is None else now
        if self.failures < self.threshold:
            return State.CLOSED
        if now - self.opened_at >= self.cooldown:
            return State.HALF_OPEN
        return State.OPEN

    def allows(self, now: float | None = None) -> bool:
        state = self.state(now)
        if state is State.CLOSED:
            return True
        if state is State.OPEN:
            return False
        # Half-open: exactly one caller gets to find out.
        if self.trial_in_flight:
            return False
        self.trial_in_flight = True
        return True

    def record_success(self) -> None:
        self.failures = 0
        self.opened_at = 0.0
        self.trial_in_flight = False

    def record_failure(self, now: float | None = None) -> None:
        now = time.monotonic() if now is None else now
        self.trial_in_flight = False
        self.failures += 1
        if self.failures >= self.threshold:
            # Re-stamp on every failure past the threshold, so a provider that
            # keeps failing its trial call keeps its full cooldown.
            self.opened_at = now


@dataclass
class RetryBudget:
    """Retries allowed as a fraction of traffic, not per request.

    Per-request retry counts multiply load exactly when a provider can least
    afford it: every caller independently decides to try three times. A budget
    caps what the whole deployment may spend on retries, so a broad outage
    degrades into fast failures instead of a stampede.
    """

    ratio: float = 0.2  # one retry per five requests
    burst: float = 5.0  # enough to cover a brief blip from cold
    tokens: float = field(default=5.0)

    def record_request(self) -> None:
        self.tokens = min(self.burst, self.tokens + self.ratio)

    def try_spend(self) -> bool:
        # Repeated additions of a ratio like 0.2 drift below the integer they
        # should land on, and a bare `< 1` would swallow the retry they paid for.
        if self.tokens < 1 - 1e-9:
            return False
        self.tokens = max(0.0, self.tokens - 1)
        return True
