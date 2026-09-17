"""Token buckets, and the two places they can live.

Two dimensions are enforced per scope, because Groq's free tier binds on the
second one first: requests per minute, and *tokens* per minute. At ~3k tokens a
call, an 8,000 token/minute ceiling allows two or three calls a minute while a
request limiter would happily wave through twenty.

Two scopes are checked per call, in one operation:

- the caller's own key, so one noisy client cannot starve the rest, and
- the provider account, because the free tier's ceiling belongs to the account
  and every caller is spending the same budget.

They must be decided together. Charging the caller and then refusing on the
account budget would bill a call that never ran, and the refund path is exactly
where a race lives.

A call costs tokens before anyone knows what it cost: the reply has not been
written yet. So the limiter reserves an estimate up front and reconciles
against the provider's real usage afterwards. An over-estimate is refunded; an
under-estimate is charged the difference, so nobody profits from prompts that
estimate badly.
"""

from __future__ import annotations

import time
from dataclasses import dataclass
from typing import Protocol


@dataclass(frozen=True)
class Limit:
    requests_per_minute: int
    tokens_per_minute: int

    @property
    def request_rate(self) -> float:
        return self.requests_per_minute / 60.0

    @property
    def token_rate(self) -> float:
        return self.tokens_per_minute / 60.0


@dataclass(frozen=True)
class Scope:
    """A bucket pair and the ceiling it enforces: the key, or the account."""

    name: str
    limit: Limit


@dataclass(frozen=True)
class Decision:
    allowed: bool
    # What bound, as "<scope>:<dimension>", so a 429 can say something true
    # rather than "too many requests" when the caller sent two.
    bound_by: str = ""
    retry_after: float = 0.0
    requests_remaining: float = 0.0
    tokens_remaining: float = 0.0
    reserved: int = 0

    def headers(self, limit: Limit) -> dict[str, str]:
        headers = {
            "ratelimit-limit": str(limit.requests_per_minute),
            "ratelimit-remaining": str(int(self.requests_remaining)),
            "ratelimit-reset": str(int(max(1, 60 - time.time() % 60))),
            "ratelimit-tokens-limit": str(limit.tokens_per_minute),
            "ratelimit-tokens-remaining": str(int(self.tokens_remaining)),
        }
        if not self.allowed:
            # Ceil: a Retry-After of 0 invites an immediate retry that is
            # guaranteed to fail again.
            headers["retry-after"] = str(max(1, int(self.retry_after + 0.999)))
            headers["ratelimit-bound-by"] = self.bound_by
        return headers


class Buckets(Protocol):
    async def take(self, scopes: list[Scope], cost: int) -> Decision: ...

    async def reconcile(self, scopes: list[Scope], reserved: int, actual: int) -> None: ...

    async def aclose(self) -> None: ...


def estimate_cost(payload: dict) -> int:
    """Guess a request's token cost before the provider has answered.

    Roughly four characters per token for the prompt, plus whatever completion
    the caller allowed. `max_tokens` is the honest reservation: if a caller
    permits 1,000 tokens of reply, the budget has to assume they meant it.
    Under-reserving is what lets a burst blow past the provider's real ceiling
    and collect 429s there instead of here.
    """
    characters = 0
    for message in payload.get("messages") or []:
        content = message.get("content") if isinstance(message, dict) else None
        if isinstance(content, str):
            characters += len(content)
        elif isinstance(content, list):  # OpenAI's multi-part content
            for part in content:
                if isinstance(part, dict) and isinstance(part.get("text"), str):
                    characters += len(part["text"])
    prompt_tokens = characters // 4
    allowance = payload.get("max_tokens") or payload.get("max_completion_tokens") or 300
    try:
        completion_tokens = max(0, int(allowance))
    except (TypeError, ValueError):
        completion_tokens = 300
    return max(1, prompt_tokens + completion_tokens)


class _Bucket:
    __slots__ = ("tokens", "updated")

    def __init__(self, tokens: float, updated: float) -> None:
        self.tokens = tokens
        self.updated = updated

    def filled(self, rate: float, capacity: float, now: float) -> float:
        return min(capacity, self.tokens + (now - self.updated) * rate)


class InMemoryBuckets:
    """Per-process buckets: the baseline this project exists to disprove.

    Every copy of the service keeps its own dictionary, so N copies admit about
    N times the limit. `scripts/measure_limit.py` measures exactly that.
    """

    def __init__(self) -> None:
        self._requests: dict[str, _Bucket] = {}
        self._tokens: dict[str, _Bucket] = {}

    async def take(self, scopes: list[Scope], cost: int) -> Decision:
        now = time.time()
        filled: list[tuple[Scope, float, float]] = []

        for scope in scopes:
            limit = scope.limit
            req = self._requests.setdefault(scope.name, _Bucket(limit.requests_per_minute, now))
            tok = self._tokens.setdefault(scope.name, _Bucket(limit.tokens_per_minute, now))
            req_filled = req.filled(limit.request_rate, limit.requests_per_minute, now)
            tok_filled = tok.filled(limit.token_rate, limit.tokens_per_minute, now)

            if req_filled < 1:
                return Decision(
                    allowed=False,
                    bound_by=f"{scope.name}:requests",
                    retry_after=(1 - req_filled) / limit.request_rate,
                    requests_remaining=max(0.0, req_filled),
                    tokens_remaining=max(0.0, tok_filled),
                )
            if tok_filled < cost:
                return Decision(
                    allowed=False,
                    bound_by=f"{scope.name}:tokens",
                    retry_after=(cost - tok_filled) / limit.token_rate,
                    requests_remaining=max(0.0, req_filled),
                    tokens_remaining=max(0.0, tok_filled),
                )
            filled.append((scope, req_filled, tok_filled))

        # Nothing is charged until every scope has said yes.
        for scope, req_filled, tok_filled in filled:
            self._requests[scope.name] = _Bucket(req_filled - 1, now)
            self._tokens[scope.name] = _Bucket(tok_filled - cost, now)

        first_req, first_tok = filled[0][1] - 1, filled[0][2] - cost
        return Decision(
            allowed=True,
            requests_remaining=first_req,
            tokens_remaining=first_tok,
            reserved=cost,
        )

    async def reconcile(self, scopes: list[Scope], reserved: int, actual: int) -> None:
        delta = reserved - actual
        if not delta:
            return
        now = time.time()
        for scope in scopes:
            bucket = self._tokens.get(scope.name)
            if bucket is None:
                continue
            filled = bucket.filled(scope.limit.token_rate, scope.limit.tokens_per_minute, now)
            # Clamping at capacity stops a run of refunds from minting budget.
            self._tokens[scope.name] = _Bucket(
                min(scope.limit.tokens_per_minute, filled + delta), now
            )

    async def aclose(self) -> None:
        return None
