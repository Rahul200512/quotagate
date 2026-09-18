"""Caching answers that can only have one right answer.

Only requests the caller declared deterministic are cached: `temperature: 0`,
no tools, no seedless sampling. Everything else is asked fresh. A cache that
guessed which answers were "close enough" would return a text the model never
produced for a prompt it never saw — a correctness bug wearing a performance
costume — which is why semantic caching is in the roadmap's *not planned*
section rather than here.

The key covers everything that changes the answer: model, messages, and the
sampling parameters. Anything not in the key is a way for two different
questions to collide on one answer.

Streamed replies are stored as the exact bytes that were relayed and replayed
as-is, so a cached stream and a live one are the same stream to the caller —
only faster.
"""

from __future__ import annotations

import hashlib
import json
import time
from collections import OrderedDict
from dataclasses import dataclass
from typing import Protocol

# Parameters that change what the model produces. `stream` is deliberately
# absent: the same question asked streamed and buffered has the same answer,
# and the stored form records which shape it was.
KEYED_FIELDS = (
    "model",
    "messages",
    "temperature",
    "top_p",
    "max_tokens",
    "max_completion_tokens",
    "stop",
    "seed",
    "response_format",
)

# A request that can come back differently next time must not be cached.
UNCACHEABLE_FIELDS = ("tools", "tool_choice", "functions", "n")


def is_cacheable(payload: dict) -> bool:
    if any(payload.get(field) for field in UNCACHEABLE_FIELDS):
        return False
    temperature = payload.get("temperature")
    # Absent temperature means the provider's default, which is not 0.
    return temperature is not None and float(temperature) == 0.0


def key_for(payload: dict) -> str:
    material = {field: payload.get(field) for field in KEYED_FIELDS}
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(encoded.encode()).hexdigest()


@dataclass(frozen=True)
class Entry:
    body: bytes
    streamed: bool
    status: int


class Cache(Protocol):
    async def get(self, key: str) -> Entry | None: ...

    async def set(self, key: str, entry: Entry, ttl: int) -> None: ...


class InMemoryCache:
    """Per-process, bounded, and therefore only as useful as one copy's traffic.

    On Vercel that is a real limit: a cache hit depends on the same instance
    answering both calls. The shared version lands with Redis; this one exists
    so the behaviour can be measured before an account does.
    """

    def __init__(self, max_entries: int = 256) -> None:
        self._entries: OrderedDict[str, tuple[Entry, float]] = OrderedDict()
        self._max = max_entries

    async def get(self, key: str) -> Entry | None:
        found = self._entries.get(key)
        if found is None:
            return None
        entry, expires_at = found
        if expires_at < time.time():
            del self._entries[key]
            return None
        self._entries.move_to_end(key)
        return entry

    async def set(self, key: str, entry: Entry, ttl: int) -> None:
        self._entries[key] = (entry, time.time() + ttl)
        self._entries.move_to_end(key)
        while len(self._entries) > self._max:
            self._entries.popitem(last=False)
