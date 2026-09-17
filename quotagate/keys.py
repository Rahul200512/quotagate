"""API keys.

Keys are compared by SHA-256 digest, so the plaintext never sits in an
environment variable, a log line or (from v1) a database row. `scripts/new_key.py`
mints one and prints the digest to configure.

v0 keeps the key set in `QUOTAGATE_KEYS`. v1 moves it to Neon, which is why
lookup already goes through one function instead of being inlined at the route.
"""

from __future__ import annotations

import hashlib
import hmac
from dataclasses import dataclass


@dataclass(frozen=True)
class ApiKey:
    name: str
    digest: str


def digest(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode()).hexdigest()


def parse_keys(raw: str) -> list[ApiKey]:
    """Parse `name:sha256,name:sha256` into keys, skipping malformed entries."""
    keys: list[ApiKey] = []
    for entry in raw.split(","):
        entry = entry.strip()
        if not entry or ":" not in entry:
            continue
        name, _, value = entry.partition(":")
        name, value = name.strip(), value.strip().lower()
        if len(value) == 64 and all(c in "0123456789abcdef" for c in value):
            keys.append(ApiKey(name=name, digest=value))
    return keys


def find(raw_key: str, keys: list[ApiKey]) -> ApiKey | None:
    """Return the matching key, comparing in constant time.

    A plain `==` on digests leaks how many leading characters matched, which is
    enough to reconstruct a digest one byte at a time given enough attempts.
    """
    candidate = digest(raw_key)
    for key in keys:
        if hmac.compare_digest(candidate, key.digest):
            return key
    return None


def bearer_token(header_value: str | None) -> str | None:
    """Pull the token out of `Authorization: Bearer <token>`."""
    if not header_value:
        return None
    scheme, _, token = header_value.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        return None
    return token.strip()
