"""Configuration, read once at import time.

Everything has a default that works without an account, so the test suite and a
local run need no secrets. Only talking to a real provider needs a key.
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _float_env(name: str, default: float) -> float:
    raw = os.environ.get(name)
    return float(raw) if raw else default


@dataclass(frozen=True)
class Settings:
    upstream_base_url: str
    upstream_api_key: str | None
    upstream_name: str
    # Connecting should fail fast; a model answering may legitimately take a
    # while, but silence *between* chunks means the stream is dead, so the read
    # timeout applies per chunk rather than to the whole response.
    connect_timeout: float
    read_timeout: float
    keys_raw: str

    @property
    def has_upstream_key(self) -> bool:
        return bool(self.upstream_api_key)


def load() -> Settings:
    return Settings(
        upstream_base_url=os.environ.get(
            "QUOTAGATE_UPSTREAM_URL", "https://api.groq.com/openai/v1"
        ).rstrip("/"),
        upstream_api_key=os.environ.get("QUOTAGATE_UPSTREAM_KEY") or os.environ.get("GROQ_API_KEY"),
        upstream_name=os.environ.get("QUOTAGATE_UPSTREAM_NAME", "groq"),
        connect_timeout=_float_env("QUOTAGATE_CONNECT_TIMEOUT", 5.0),
        read_timeout=_float_env("QUOTAGATE_READ_TIMEOUT", 45.0),
        keys_raw=os.environ.get("QUOTAGATE_KEYS", ""),
    )


settings = load()
