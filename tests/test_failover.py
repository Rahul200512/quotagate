"""Failover, the circuit breaker, and the retry budget — over real sockets.

Two fake providers run side by side. `alpha` is asked first; `beta` only ever
answers when alpha could not.
"""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from quotagate.keys import digest
from tests.servers import running

DEMO_KEY = "qg_test_key"
AUTH = {"authorization": f"Bearer {DEMO_KEY}"}
DEAD_URL = "http://127.0.0.1:1/v1"


def body(model: str = "fake", **extra) -> dict:
    return {"model": model, "messages": [{"role": "user", "content": "hi"}], **extra}


def env_for(alpha: str, beta: str, **overrides: str) -> dict[str, str]:
    env = {
        "QUOTAGATE_KEYS": f"tests:{digest(DEMO_KEY)}",
        "QUOTAGATE_PROVIDERS": "alpha,beta",
        "QUOTAGATE_PROVIDER_ALPHA_URL": alpha,
        "QUOTAGATE_PROVIDER_ALPHA_KEY": "alpha-key",
        "QUOTAGATE_PROVIDER_BETA_URL": beta,
        "QUOTAGATE_PROVIDER_BETA_KEY": "beta-key",
        "QUOTAGATE_KEY_RPM": "1000",
        "QUOTAGATE_KEY_TPM": "10000000",
        "QUOTAGATE_ACCOUNT_RPM": "1000",
        "QUOTAGATE_ACCOUNT_TPM": "10000000",
        "QUOTAGATE_CONNECT_TIMEOUT": "1",
    }
    env.update(overrides)
    return env


@pytest.fixture(scope="module")
def alpha() -> Iterator[str]:
    with running("tests.fake_upstream:app", env={"FAKE_NAME": "alpha"}) as url:
        yield f"{url}/v1"


@pytest.fixture(scope="module")
def beta() -> Iterator[str]:
    with running("tests.fake_upstream:app", env={"FAKE_NAME": "beta"}) as url:
        yield f"{url}/v1"


@pytest.fixture(scope="module")
def gateway(alpha: str, beta: str) -> Iterator[str]:
    with running("app:app", env=env_for(alpha, beta)) as url:
        yield url


def post(gateway: str, payload: dict, timeout: float = 20) -> httpx.Response:
    return httpx.post(f"{gateway}/v1/chat/completions", json=payload, headers=AUTH, timeout=timeout)


def test_the_first_provider_answers_when_it_is_healthy(gateway: str) -> None:
    response = post(gateway, body())
    assert response.status_code == 200
    assert response.headers["x-quotagate-provider"] == "alpha"
    assert response.json()["answered_by"] == "alpha"


def test_a_provider_error_moves_to_the_next_provider(gateway: str) -> None:
    response = post(gateway, body("alpha:fake-500"))
    assert response.status_code == 200
    assert response.headers["x-quotagate-provider"] == "beta"
    assert response.headers["x-quotagate-attempts"] == "alpha:500,beta:200"


def test_being_rate_limited_by_one_provider_moves_to_the_other(gateway: str) -> None:
    """Another provider may have budget left even when this one does not."""
    response = post(gateway, body("alpha:fake-429"))
    assert response.status_code == 200
    assert response.headers["x-quotagate-provider"] == "beta"


def test_a_bad_request_is_not_retried_anywhere(gateway: str) -> None:
    """A 400 is the caller's fault; a second provider would reject it too."""
    response = post(gateway, body("alpha:fake-400"))
    assert response.status_code == 400
    assert response.headers["x-quotagate-provider"] == "alpha"
    assert "beta" not in response.headers.get("x-quotagate-attempts", "")


def test_an_unreachable_provider_is_skipped(beta: str) -> None:
    with running("app:app", env=env_for(DEAD_URL, beta)) as url:
        response = post(url, body())
    assert response.status_code == 200
    assert response.headers["x-quotagate-provider"] == "beta"
    assert response.headers["x-quotagate-attempts"].startswith("alpha:cannot reach")


def test_a_stream_that_dies_midway_is_not_restarted_on_another_provider(gateway: str) -> None:
    """Once tokens have been sent, no other model can honestly continue them."""
    received = []
    with httpx.Client(timeout=20) as client:
        with client.stream(
            "POST",
            f"{gateway}/v1/chat/completions",
            json=body("alpha:fake-die", stream=True),
            headers=AUTH,
        ) as response:
            assert response.status_code == 200
            assert response.headers["x-quotagate-provider"] == "alpha"
            try:
                for line in response.iter_lines():
                    if line.startswith("data: "):
                        received.append(line)
            except httpx.HTTPError:
                pass  # the provider hung up mid-stream, which is the point

    assert received, "the caller should keep the tokens that did arrive"
    assert not any("[DONE]" in line for line in received), "a broken stream must not look complete"


def test_a_failing_provider_is_dropped_from_the_rotation(alpha: str, beta: str) -> None:
    """After enough consecutive failures, stop asking and fail fast."""
    # Every request earns a retry here, so the budget cannot end the run early
    # and hide what the breaker did.
    with running("app:app", env=env_for(alpha, beta, QUOTAGATE_RETRY_RATIO="1")) as url:
        for _ in range(6):
            post(url, body("alpha:fake-500"))
        health = httpx.get(f"{url}/healthz", timeout=10).json()
        after = post(url, body("alpha:fake-500"))

    circuits = {p["name"]: p["circuit"] for p in health["providers"]}
    assert circuits["alpha"] == "open"
    assert after.headers["x-quotagate-attempts"].startswith("alpha:skipped(circuit_open)")
    assert after.headers["x-quotagate-provider"] == "beta"


def test_the_retry_budget_stops_a_stampede_when_everything_is_failing() -> None:
    """With both providers down, the gateway must not keep doubling its load."""
    # The breaker is held back so this measures the budget, not the breaker.
    env = env_for(DEAD_URL, DEAD_URL, QUOTAGATE_BREAKER_THRESHOLD="100")
    with running("app:app", env=env) as url:
        attempts = [
            post(url, body(), timeout=10).headers.get("x-quotagate-attempts", "")
            for _ in range(12)
        ]

    assert any("no_retry_budget" in attempt for attempt in attempts), attempts
    assert attempts[0].count("cannot reach") == 2, "the first request should still try both"


def test_a_model_only_the_backup_serves_goes_straight_to_the_backup(alpha: str, beta: str) -> None:
    """Providers name the same weights differently; asking the wrong one 404s."""
    env = env_for(
        alpha,
        beta,
        QUOTAGATE_PROVIDER_ALPHA_MODELS="house-model=fake",
        QUOTAGATE_PROVIDER_BETA_MODELS="house-model=fake,rare-model=fake",
    )
    with running("app:app", env=env) as url:
        common = post(url, body("house-model"))
        rare = post(url, body("rare-model"))

    assert common.headers["x-quotagate-provider"] == "alpha"
    assert rare.status_code == 200
    assert rare.headers["x-quotagate-provider"] == "beta"
    assert rare.headers["x-quotagate-attempts"] == "alpha:skipped(model_unavailable),beta:200"


def test_the_id_the_provider_knows_is_the_one_it_is_sent(alpha: str, beta: str) -> None:
    env = env_for(alpha, beta, QUOTAGATE_PROVIDER_ALPHA_MODELS="house-model=alpha:fake")
    with running("app:app", env=env) as url:
        response = post(url, body("house-model"))

    assert response.status_code == 200
    # The fake echoes back the model it was actually asked for.
    assert response.json()["model"] == "fake"
    assert response.json()["answered_by"] == "alpha"


def test_a_model_no_provider_serves_is_a_404_not_a_dead_end(alpha: str, beta: str) -> None:
    env = env_for(
        alpha,
        beta,
        QUOTAGATE_PROVIDER_ALPHA_MODELS="house-model=fake",
        QUOTAGATE_PROVIDER_BETA_MODELS="house-model=fake",
    )
    with running("app:app", env=env) as url:
        response = post(url, body("nobody-has-this"))

    assert response.status_code == 404
    assert response.json()["error"]["code"] == "model_not_found"
    assert "skipped(model_unavailable)" in response.headers["x-quotagate-attempts"]


def test_skipping_for_a_missing_model_does_not_count_against_the_provider(
    alpha: str, beta: str
) -> None:
    """A model this provider never had is not evidence that it is unhealthy."""
    env = env_for(alpha, beta, QUOTAGATE_PROVIDER_ALPHA_MODELS="house-model=fake")
    with running("app:app", env=env) as url:
        for _ in range(8):
            post(url, body("rare-model"))
        health = httpx.get(f"{url}/healthz", timeout=10).json()

    circuits = {p["name"]: p["circuit"] for p in health["providers"]}
    assert circuits["alpha"] == "closed"
