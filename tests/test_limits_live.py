"""The limiter as a caller meets it: statuses, headers, and failure modes."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from quotagate.keys import digest
from tests.servers import running

DEMO_KEY = "qg_test_key"
AUTH = {"authorization": f"Bearer {DEMO_KEY}"}
BODY = {"model": "fake", "messages": [{"role": "user", "content": "hi"}]}


def gateway_env(upstream_url: str, **overrides: str) -> dict[str, str]:
    env = {
        "QUOTAGATE_UPSTREAM_URL": f"{upstream_url}/v1",
        "QUOTAGATE_UPSTREAM_KEY": "not-a-real-key",
        "QUOTAGATE_UPSTREAM_NAME": "fake",
        "QUOTAGATE_KEYS": f"tests:{digest(DEMO_KEY)}",
        "QUOTAGATE_KEY_RPM": "3",
        "QUOTAGATE_KEY_TPM": "1000000",
        "QUOTAGATE_ACCOUNT_RPM": "1000",
        "QUOTAGATE_ACCOUNT_TPM": "1000000",
    }
    env.update(overrides)
    return env


@pytest.fixture(scope="module")
def upstream_url() -> Iterator[str]:
    with running("tests.fake_upstream:app") as url:
        yield url


@pytest.fixture(scope="module")
def gateway(upstream_url: str) -> Iterator[str]:
    with running("app:app", env=gateway_env(upstream_url)) as url:
        yield url


def test_a_burst_is_cut_off_at_the_limit(gateway: str) -> None:
    statuses = [
        httpx.post(f"{gateway}/v1/chat/completions", json=BODY, headers=AUTH, timeout=20).status_code
        for _ in range(5)
    ]
    assert statuses.count(200) == 3
    assert statuses.count(429) == 2


def test_a_refusal_explains_itself(gateway: str) -> None:
    for _ in range(6):
        response = httpx.post(
            f"{gateway}/v1/chat/completions", json=BODY, headers=AUTH, timeout=20
        )
        if response.status_code == 429:
            break
    assert response.status_code == 429
    assert int(response.headers["retry-after"]) >= 1
    assert response.headers["ratelimit-bound-by"].endswith(":requests")
    assert response.json()["error"]["type"] == "rate_limit_exceeded"


def test_successful_replies_carry_the_remaining_budget(upstream_url: str) -> None:
    with running("app:app", env=gateway_env(upstream_url, QUOTAGATE_KEY_RPM="50")) as url:
        response = httpx.post(f"{url}/v1/chat/completions", json=BODY, headers=AUTH, timeout=20)
        streamed = httpx.post(
            f"{url}/v1/chat/completions", json={**BODY, "stream": True}, headers=AUTH, timeout=20
        )
    assert response.headers["ratelimit-limit"] == "50"
    assert int(response.headers["ratelimit-remaining"]) < 50
    assert "ratelimit-tokens-remaining" in streamed.headers


def test_an_unreachable_limiter_refuses_by_default(upstream_url: str) -> None:
    """Fail closed: an unchecked request spends a budget everyone shares."""
    env = gateway_env(upstream_url, QUOTAGATE_REDIS_REST_URL="http://127.0.0.1:1", QUOTAGATE_REDIS_REST_TOKEN="x")
    with running("app:app", env=env) as url:
        response = httpx.post(f"{url}/v1/chat/completions", json=BODY, headers=AUTH, timeout=20)
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "limiter_unavailable"


def test_fail_open_is_available_but_must_be_asked_for(upstream_url: str) -> None:
    env = gateway_env(
        upstream_url,
        QUOTAGATE_REDIS_REST_URL="http://127.0.0.1:1",
        QUOTAGATE_REDIS_REST_TOKEN="x",
        QUOTAGATE_FAIL_OPEN="true",
    )
    with running("app:app", env=env) as url:
        response = httpx.post(f"{url}/v1/chat/completions", json=BODY, headers=AUTH, timeout=20)
    assert response.status_code == 200


def test_health_reports_which_promise_is_in_force(upstream_url: str) -> None:
    with running("app:app", env=gateway_env(upstream_url)) as per_process:
        local = httpx.get(f"{per_process}/healthz", timeout=10).json()
    env = gateway_env(upstream_url, QUOTAGATE_REDIS_REST_URL="https://example.invalid", QUOTAGATE_REDIS_REST_TOKEN="x")
    with running("app:app", env=env) as shared:
        remote = httpx.get(f"{shared}/healthz", timeout=10).json()

    assert local["limits"]["scope"] == "per-process"
    assert remote["limits"]["scope"] == "shared"
    assert local["limits"]["on_limiter_failure"] == "refuse"


def test_limit_check_spends_the_same_budget_as_a_real_call(gateway: str) -> None:
    """The measurement endpoint has to share buckets with the proxy, or it
    would measure a limiter nobody is subject to."""
    first = httpx.post(f"{gateway}/debug/limit-check?cost=1", headers=AUTH, timeout=10)
    assert first.status_code in (200, 429)
    statuses = [
        httpx.post(f"{gateway}/debug/limit-check?cost=1", headers=AUTH, timeout=10).status_code
        for _ in range(6)
    ]
    assert 429 in statuses
    assert httpx.post(f"{gateway}/v1/chat/completions", json=BODY, headers=AUTH, timeout=20).status_code == 429


def test_two_copies_without_shared_state_admit_twice_the_limit(upstream_url: str) -> None:
    """The failure this project exists to fix, measured end to end."""
    env = gateway_env(upstream_url, QUOTAGATE_KEY_RPM="3")
    with running("app:app", env=env) as copy_a, running("app:app", env=env) as copy_b:
        admitted = 0
        for index in range(12):
            url = copy_a if index % 2 == 0 else copy_b
            response = httpx.post(f"{url}/debug/limit-check?cost=1", headers=AUTH, timeout=10)
            admitted += response.status_code == 200

    assert admitted == 6, "two per-process limiters should each admit the full 3"
