"""Which provider to ask, in what order, and when to give up on one.

Failover is only attempted **before the first byte reaches the caller**. Once
tokens have been sent, the caller holds half an answer; a second provider would
continue it in a different voice, from a different model, with no shared state.
A gateway that did that would be producing a text no single model wrote, which
is worse than an honest error.

Which failures are worth moving on for:

- the provider could not be reached, or timed out — try the next one
- 5xx — the provider says it is broken; try the next one
- 429 — it is rate limiting *us*; another provider may not be, so try
- 4xx — the caller's request is wrong; a second provider would reject it the
  same way, so return it unchanged and do not blame the provider for it
"""

from __future__ import annotations

from dataclasses import dataclass, field

import httpx

from quotagate.resilience import CircuitBreaker


@dataclass
class Provider:
    name: str
    base_url: str
    api_key: str | None
    client: httpx.AsyncClient | None = None
    breaker: CircuitBreaker = field(default_factory=CircuitBreaker)
    # What this provider calls each model. Empty means "send it unchanged".
    models: dict[str, str] = field(default_factory=dict)

    def serves(self, model: str) -> str | None:
        """The id this provider knows the model by, or None if it has none.

        A provider with a map is making a claim about what it serves, so an
        unlisted model means skip it — asking anyway would trade a useful
        failover for a 404 and an opened circuit.
        """
        if not self.models:
            return model
        return self.models.get(model)


RETRYABLE_STATUS = {408, 409, 425, 429, 500, 502, 503, 504}


def should_try_another(status: int) -> bool:
    return status in RETRYABLE_STATUS or status >= 500


@dataclass
class Attempt:
    """What happened with one provider, for the request log and the tests."""

    provider: str
    status: int | None = None
    error: str | None = None
    skipped: bool = False

    def as_text(self) -> str:
        if self.skipped:
            # Why it was skipped matters as much as that it was: a circuit that
            # is open and a budget that is spent need different fixes.
            return f"{self.provider}:skipped({self.error})" if self.error else f"{self.provider}:skipped"
        if self.error:
            return f"{self.provider}:{self.error}"
        return f"{self.provider}:{self.status}"
