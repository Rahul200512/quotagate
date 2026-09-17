"""The same buckets, shared by every copy of the service.

The whole check-and-deduct runs inside one Lua script. Doing it in Python would
mean read, decide, write — and two copies interleaving those three steps both
read the same remaining budget and both spend it. That race is not theoretical:
it is the bug this project exists to fix, and it reappears the moment the
decision leaves the server.

Two transports, because the same code runs in two places:

- `RestTransport` speaks Upstash's HTTP API. Serverless copies come and go, and
  an HTTP call keeps no connection alive, so a burst of new instances cannot
  exhaust a connection limit.
- `TcpTransport` speaks the Redis protocol, so the script can be tested against
  a real Redis on a laptop without an account.
"""

from __future__ import annotations

import time
from typing import Any, Protocol

import httpx

from quotagate.limits import Decision, Scope

# KEYS: for each scope, its requests key then its tokens key.
# ARGV: now, cost, ttl, then requests_per_minute and tokens_per_minute per scope.
TAKE = """
local now  = tonumber(ARGV[1])
local cost = tonumber(ARGV[2])
local ttl  = tonumber(ARGV[3])
local scopes = #KEYS / 2

local function filled(key, capacity, rate)
  local state = redis.call('HMGET', key, 'tokens', 'updated')
  local tokens = tonumber(state[1])
  if tokens == nil then return capacity end
  local updated = tonumber(state[2]) or now
  local value = tokens + (now - updated) * rate
  if value > capacity then value = capacity end
  return value
end

local req_filled, tok_filled = {}, {}

-- Every scope is checked before any scope is charged. Charging the caller and
-- then refusing on the account budget would bill a call that never ran.
for i = 1, scopes do
  local rpm = tonumber(ARGV[3 + (i - 1) * 2 + 1])
  local tpm = tonumber(ARGV[3 + (i - 1) * 2 + 2])
  local req = filled(KEYS[i * 2 - 1], rpm, rpm / 60.0)
  local tok = filled(KEYS[i * 2], tpm, tpm / 60.0)
  if req < 1 then
    return {0, tostring(i), 'requests', tostring((1 - req) * 60.0 / rpm),
            tostring(req), tostring(tok)}
  end
  if tok < cost then
    return {0, tostring(i), 'tokens', tostring((cost - tok) * 60.0 / tpm),
            tostring(req), tostring(tok)}
  end
  req_filled[i] = req
  tok_filled[i] = tok
end

for i = 1, scopes do
  redis.call('HSET', KEYS[i * 2 - 1], 'tokens', tostring(req_filled[i] - 1), 'updated', tostring(now))
  redis.call('HSET', KEYS[i * 2], 'tokens', tostring(tok_filled[i] - cost), 'updated', tostring(now))
  redis.call('EXPIRE', KEYS[i * 2 - 1], ttl)
  redis.call('EXPIRE', KEYS[i * 2], ttl)
end

return {1, '0', '', '0', tostring(req_filled[1] - 1), tostring(tok_filled[1] - cost)}
"""

# KEYS: the tokens key of each scope. ARGV: now, delta, ttl, then tpm per scope.
RECONCILE = """
local now   = tonumber(ARGV[1])
local delta = tonumber(ARGV[2])
local ttl   = tonumber(ARGV[3])
for i = 1, #KEYS do
  local tpm = tonumber(ARGV[3 + i])
  local state = redis.call('HMGET', KEYS[i], 'tokens', 'updated')
  local tokens = tonumber(state[1])
  if tokens ~= nil then
    local updated = tonumber(state[2]) or now
    local value = tokens + (now - updated) * (tpm / 60.0) + delta
    if value > tpm then value = tpm end
    redis.call('HSET', KEYS[i], 'tokens', tostring(value), 'updated', tostring(now))
    redis.call('EXPIRE', KEYS[i], ttl)
  end
end
return 1
"""

TTL_SECONDS = 180


class Transport(Protocol):
    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any: ...

    async def aclose(self) -> None: ...


class RestTransport:
    """Upstash's HTTP API. Stateless, so new instances cost no connections."""

    def __init__(self, url: str, token: str, client: httpx.AsyncClient | None = None) -> None:
        self._url = url.rstrip("/")
        self._token = token
        self._client = client or httpx.AsyncClient(
            timeout=httpx.Timeout(connect=2.0, read=3.0, write=3.0, pool=2.0)
        )

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        command = ["EVAL", script, str(len(keys)), *keys, *args]
        response = await self._client.post(
            self._url, json=command, headers={"authorization": f"Bearer {self._token}"}
        )
        response.raise_for_status()
        return response.json().get("result")

    async def aclose(self) -> None:
        await self._client.aclose()


class TcpTransport:
    """Redis over its own protocol, for testing against a local server."""

    def __init__(self, url: str) -> None:
        import redis.asyncio as redis  # lazy: not needed in production

        self._client = redis.from_url(url, decode_responses=True)

    async def eval(self, script: str, keys: list[str], args: list[str]) -> Any:
        return await self._client.eval(script, len(keys), *keys, *args)

    async def aclose(self) -> None:
        await self._client.aclose()


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, (bytes, bytearray)) else str(value)


class RedisBuckets:
    def __init__(self, transport: Transport, namespace: str = "qg") -> None:
        self._transport = transport
        self._namespace = namespace

    def _keys(self, scopes: list[Scope]) -> list[str]:
        keys: list[str] = []
        for scope in scopes:
            # A Lua script may only touch keys that live on one node, so both
            # buckets of a scope share a hash tag.
            keys.append(f"{self._namespace}:{{{scope.name}}}:req")
            keys.append(f"{self._namespace}:{{{scope.name}}}:tok")
        return keys

    async def take(self, scopes: list[Scope], cost: int) -> Decision:
        args = [f"{time.time():.6f}", str(cost), str(TTL_SECONDS)]
        for scope in scopes:
            args += [str(scope.limit.requests_per_minute), str(scope.limit.tokens_per_minute)]

        raw = await self._transport.eval(TAKE, self._keys(scopes), args)
        allowed = bool(int(_text(raw[0])))
        bound_by = ""
        if not allowed:
            index = int(_text(raw[1])) - 1
            bound_by = f"{scopes[index].name}:{_text(raw[2])}"
        return Decision(
            allowed=allowed,
            bound_by=bound_by,
            retry_after=float(_text(raw[3])),
            requests_remaining=max(0.0, float(_text(raw[4]))),
            tokens_remaining=max(0.0, float(_text(raw[5]))),
            reserved=cost if allowed else 0,
        )

    async def reconcile(self, scopes: list[Scope], reserved: int, actual: int) -> None:
        delta = reserved - actual
        if not delta:
            return
        keys = [f"{self._namespace}:{{{scope.name}}}:tok" for scope in scopes]
        args = [f"{time.time():.6f}", str(delta), str(TTL_SECONDS)]
        args += [str(scope.limit.tokens_per_minute) for scope in scopes]
        await self._transport.eval(RECONCILE, keys, args)

    async def aclose(self) -> None:
        await self._transport.aclose()
