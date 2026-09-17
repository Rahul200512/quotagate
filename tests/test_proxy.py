"""The proxy, against a real provider-shaped server on a real socket."""

from __future__ import annotations

import time
from collections.abc import Iterator

import httpx
import pytest

from quotagate.keys import digest
from tests.servers import running

DEMO_KEY = "qg_test_key"
KEYS = f"tests:{digest(DEMO_KEY)}"
AUTH = {"authorization": f"Bearer {DEMO_KEY}"}


@pytest.fixture(scope="module")
def upstream_url() -> Iterator[str]:
    with running("tests.fake_upstream:app") as url:
        yield url


@pytest.fixture(scope="module")
def gateway(upstream_url: str) -> Iterator[str]:
    env = {
        "QUOTAGATE_UPSTREAM_URL": f"{upstream_url}/v1",
        "QUOTAGATE_UPSTREAM_KEY": "not-a-real-key",
        "QUOTAGATE_UPSTREAM_NAME": "fake",
        "QUOTAGATE_KEYS": KEYS,
        # The limiter has its own tests; keep it out of the way here.
        "QUOTAGATE_KEY_RPM": "1000",
        "QUOTAGATE_KEY_TPM": "1000000",
        "QUOTAGATE_ACCOUNT_RPM": "1000",
        "QUOTAGATE_ACCOUNT_TPM": "1000000",
    }
    with running("app:app", env=env) as url:
        yield url


def chat(gateway: str, **payload) -> httpx.Response:
    body = {"model": "fake", "messages": [{"role": "user", "content": "hi"}], **payload}
    return httpx.post(f"{gateway}/v1/chat/completions", json=body, headers=AUTH, timeout=30)


def test_buffered_completion_passes_through(gateway: str) -> None:
    response = chat(gateway)
    assert response.status_code == 200
    body = response.json()
    assert body["usage"]["total_tokens"] == 18
    assert body["choices"][0]["message"]["content"]


def test_streamed_tokens_arrive_as_they_are_produced(gateway: str) -> None:
    """The point of the gateway: it must not hold tokens back."""
    arrivals: list[float] = []
    started = time.perf_counter()
    with httpx.Client(timeout=30) as client:
        with client.stream(
            "POST",
            f"{gateway}/v1/chat/completions",
            json={"model": "fake", "messages": [{"role": "user", "content": "hi"}], "stream": True},
            headers=AUTH,
        ) as response:
            assert response.status_code == 200
            assert response.headers["content-type"].startswith("text/event-stream")
            for line in response.iter_lines():
                if line.startswith("data: "):
                    arrivals.append((time.perf_counter() - started) * 1000)

    assert arrivals, "no events arrived"
    gaps = [b - a for a, b in zip(arrivals, arrivals[1:])]
    # The fake provider pauses 50ms between chunks; a gateway that buffered
    # would collapse every gap to roughly zero.
    assert max(gaps) > 25, f"stream looks buffered: {gaps}"


def test_rate_limited_upstream_keeps_its_status_and_retry_after(gateway: str) -> None:
    response = chat(gateway, model="fake-429")
    assert response.status_code == 429
    assert response.headers["retry-after"] == "3"
    assert response.json()["error"]["type"] == "rate_limit_exceeded"


def test_non_json_upstream_error_is_still_an_openai_shaped_error(gateway: str) -> None:
    response = chat(gateway, model="fake-500")
    assert response.status_code == 500
    assert "upstream exploded" in response.json()["error"]["message"]


def test_missing_key_is_rejected(gateway: str) -> None:
    response = httpx.post(
        f"{gateway}/v1/chat/completions",
        json={"model": "fake", "messages": [{"role": "user", "content": "hi"}]},
        timeout=10,
    )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "missing_api_key"


def test_wrong_key_is_rejected(gateway: str) -> None:
    response = httpx.post(
        f"{gateway}/v1/chat/completions",
        json={"model": "fake", "messages": [{"role": "user", "content": "hi"}]},
        headers={"authorization": "Bearer qg_not_the_key"},
        timeout=10,
    )
    assert response.status_code == 401
    assert response.json()["error"]["code"] == "invalid_api_key"


def test_malformed_requests_are_rejected_before_the_provider_is_called(gateway: str) -> None:
    bad_json = httpx.post(
        f"{gateway}/v1/chat/completions", content=b"{not json", headers=AUTH, timeout=10
    )
    missing_fields = httpx.post(
        f"{gateway}/v1/chat/completions", json={"model": "fake"}, headers=AUTH, timeout=10
    )
    assert bad_json.status_code == 400
    assert missing_fields.status_code == 400


def test_oversized_body_is_refused(gateway: str) -> None:
    huge = "x" * (300 * 1024)
    response = chat(gateway, messages=[{"role": "user", "content": huge}])
    assert response.status_code == 413


def test_models_list_is_proxied(gateway: str) -> None:
    response = httpx.get(f"{gateway}/v1/models", headers=AUTH, timeout=10)
    assert response.status_code == 200
    assert response.json()["data"][0]["id"] == "fake"


def test_gateway_with_no_keys_refuses_instead_of_standing_open() -> None:
    """An open gateway in front of a real provider key is the worse failure."""
    with running("app:app", env={"QUOTAGATE_KEYS": ""}) as url:
        response = httpx.post(
            f"{url}/v1/chat/completions",
            json={"model": "fake", "messages": [{"role": "user", "content": "hi"}]},
            timeout=10,
        )
    assert response.status_code == 503
    assert response.json()["error"]["code"] == "not_configured"


def test_client_disconnect_does_not_wedge_the_gateway(gateway: str) -> None:
    with httpx.Client(timeout=30) as client:
        with client.stream(
            "POST",
            f"{gateway}/v1/chat/completions",
            json={"model": "fake", "messages": [{"role": "user", "content": "hi"}], "stream": True},
            headers=AUTH,
        ) as response:
            for line in response.iter_lines():
                if line.startswith("data: "):
                    break  # hang up after the first token

    assert httpx.get(f"{gateway}/healthz", timeout=5).json()["status"] == "ok"
