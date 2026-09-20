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
    demo_key: str

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
        # The demo key is published on the front page on purpose: it is the
        # thing a visitor tries the gateway with, and its small quota is the
        # feature being demonstrated. It is a key, not a secret.
        demo_key=os.environ.get("QUOTAGATE_DEMO_KEY", ""),
    )


settings = load()


def _int_env(name: str, default: int) -> int:
    raw = os.environ.get(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


@dataclass(frozen=True)
class LimitSettings:
    """Ceilings, and what to do when the limiter itself is unreachable."""

    key_rpm: int
    key_tpm: int
    # Groq's free tier is 8,000 tokens/minute for the *account*, so the
    # account ceiling sits below it, leaving room for the estimate to be wrong.
    account_rpm: int
    account_tpm: int
    redis_rest_url: str | None
    redis_rest_token: str | None
    redis_url: str | None
    # Default closed: if the limiter cannot be consulted, refusing costs a
    # request, while allowing costs the provider budget everyone shares.
    fail_open: bool

    @property
    def shared(self) -> bool:
        return bool((self.redis_rest_url and self.redis_rest_token) or self.redis_url)


def load_limits() -> LimitSettings:
    return LimitSettings(
        key_rpm=_int_env("QUOTAGATE_KEY_RPM", 10),
        key_tpm=_int_env("QUOTAGATE_KEY_TPM", 6_000),
        account_rpm=_int_env("QUOTAGATE_ACCOUNT_RPM", 25),
        account_tpm=_int_env("QUOTAGATE_ACCOUNT_TPM", 7_000),
        redis_rest_url=os.environ.get("QUOTAGATE_REDIS_REST_URL") or None,
        redis_rest_token=os.environ.get("QUOTAGATE_REDIS_REST_TOKEN") or None,
        redis_url=os.environ.get("QUOTAGATE_REDIS_URL") or None,
        fail_open=os.environ.get("QUOTAGATE_FAIL_OPEN", "").lower() in {"1", "true", "yes"},
    )


limits = load_limits()


@dataclass(frozen=True)
class CacheSettings:
    enabled: bool
    ttl_seconds: int
    # A reply larger than this is streamed to the caller but not stored; the
    # memory a cache may hold has to be bounded by something.
    max_bytes: int


def load_cache() -> CacheSettings:
    return CacheSettings(
        enabled=os.environ.get("QUOTAGATE_CACHE", "on").lower() not in {"0", "off", "false"},
        ttl_seconds=_int_env("QUOTAGATE_CACHE_TTL", 300),
        max_bytes=_int_env("QUOTAGATE_CACHE_MAX_BYTES", 256 * 1024),
    )


cache = load_cache()


@dataclass(frozen=True)
class ResilienceSettings:
    """Thresholds worth tuning per deployment without a code change."""

    breaker_threshold: int
    breaker_cooldown: float
    retry_ratio: float
    retry_burst: float
    # The shared budget refills from the clock, so it needs a rate of its own
    # rather than a fraction of traffic.
    retry_per_minute: float


def load_resilience() -> ResilienceSettings:
    return ResilienceSettings(
        breaker_threshold=_int_env("QUOTAGATE_BREAKER_THRESHOLD", 5),
        breaker_cooldown=_float_env("QUOTAGATE_BREAKER_COOLDOWN", 20.0),
        retry_ratio=_float_env("QUOTAGATE_RETRY_RATIO", 0.2),
        retry_burst=_float_env("QUOTAGATE_RETRY_BURST", 5.0),
        retry_per_minute=_float_env("QUOTAGATE_RETRY_PER_MINUTE", 12.0),
    )


resilience = load_resilience()


def parse_models(raw: str) -> dict[str, str]:
    """`asked=served,asked=served` — what this provider calls each model.

    Providers name the same weights differently, and a provider that has never
    heard of an id answers 404 rather than failing over usefully. An empty map
    means "send the id through unchanged", which is right for the provider the
    caller is naming models for.
    """
    mapping: dict[str, str] = {}
    for pair in raw.split(","):
        asked, _, served = pair.partition("=")
        asked, served = asked.strip(), served.strip()
        if asked and served:
            mapping[asked] = served
    return mapping


@dataclass(frozen=True)
class ProviderSpec:
    name: str
    base_url: str
    api_key: str | None
    models: dict[str, str]


def load_providers() -> list[ProviderSpec]:
    """Providers in preference order.

    `QUOTAGATE_PROVIDERS=groq,openrouter` plus `QUOTAGATE_PROVIDER_GROQ_URL`
    and `QUOTAGATE_PROVIDER_GROQ_KEY` for each. With nothing set, the single
    upstream from `QUOTAGATE_UPSTREAM_*` is the only provider, so a deployment
    that never wanted failover keeps working unchanged.
    """
    names = [n.strip() for n in os.environ.get("QUOTAGATE_PROVIDERS", "").split(",") if n.strip()]
    if not names:
        return [
            ProviderSpec(
                settings.upstream_name, settings.upstream_base_url, settings.upstream_api_key, {}
            )
        ]

    specs: list[ProviderSpec] = []
    for name in names:
        prefix = f"QUOTAGATE_PROVIDER_{name.upper().replace('-', '_')}"
        url = os.environ.get(f"{prefix}_URL")
        if not url:
            continue  # a named provider with no URL is a typo, not a provider
        specs.append(
            ProviderSpec(
                name=name,
                base_url=url.rstrip("/"),
                api_key=os.environ.get(f"{prefix}_KEY") or None,
                models=parse_models(os.environ.get(f"{prefix}_MODELS", "")),
            )
        )
    return specs or [
        ProviderSpec(
            settings.upstream_name, settings.upstream_base_url, settings.upstream_api_key, {}
        )
    ]


providers = load_providers()
