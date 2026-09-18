# quotagate

An OpenAI-compatible gateway that puts shared rate limits, failover and usage
accounting in front of a model provider.

[![CI](https://github.com/Rahul200512/quotagate/actions/workflows/ci.yml/badge.svg)](https://github.com/Rahul200512/quotagate/actions/workflows/ci.yml)

Live: **https://quotagate.vercel.app** — the front page runs the streaming probe
and a real completion in your browser, against a public demo key.

## Why

BumpCheck taught me that a token budget is easy to get right in one process and
impossible to get right anywhere else. Its limiter reserves an estimate before
a call and reconciles against the real usage afterwards, and it works, because
exactly one Python process ever holds the budget. The moment a second copy
starts, both copies believe they own the whole 8,000 tokens per minute that
Groq's free tier gives the *account*, and the account starts collecting 429s.

I have hit the deployed version of this before. A per-IP limiter I shipped
earlier did nothing in production, because every request arrived from the load
balancer and they all shared one bucket key. It passed locally. It passed in
tests. It silently let everything through in the only place that mattered.

So this is the piece I keep needing and keep writing badly: one gateway in
front of the model, holding the limits in Redis where every copy of the service
can see them, cancelling upstream work when the caller walks away, failing over
when a provider breaks, and keeping a usage ledger that reconciles against its
own request log. Uber runs a Go service shaped like this in front of its model
vendors; Cloudflare and Kong sell one. This is the small version, deployed, and
measured against the deployment rather than against my laptop.

## Try it

```
curl -N https://quotagate.vercel.app/v1/chat/completions \
  -H "authorization: Bearer qg_demo_jofwzC2Z1Babr3KVM7DRh68K" \
  -H "content-type: application/json" \
  -d '{"model":"openai/gpt-oss-20b","stream":true,
       "messages":[{"role":"user","content":"hello"}]}'
```

That key is public on purpose. It is capped at 10 requests and 6,000 tokens a
minute, shared by everyone who reads this, so leaning on it demonstrates the
limiter rather than costing me anything.

Locally:

```
.venv/bin/python scripts/new_key.py demo
QUOTAGATE_KEYS="demo:<digest>" QUOTAGATE_UPSTREAM_KEY="$GROQ_API_KEY" \
  .venv/bin/python -m uvicorn app:app
```

```
curl -N http://127.0.0.1:8000/v1/chat/completions \
  -H "authorization: Bearer qg_..." -H "content-type: application/json" \
  -d '{"model":"openai/gpt-oss-20b","stream":true,
       "messages":[{"role":"user","content":"hello"}]}'
```

The first thing this repo answers, though, is whether the platform I chose can
stream at all:

```
python scripts/measure_stream.py http://127.0.0.1:8000
```

```
url                http://127.0.0.1:8000/debug/stream?chunks=20&gap_ms=200
content-type       text/event-stream; charset=utf-8
events             20
time to first event 67 ms
gap requested      200 ms
gap median         201 ms
gap min / max      200 / 202 ms
total              3891 ms
verdict            STREAMED
```

## v0 — the streaming spine

A gateway that buffers is not a gateway; the caller waits for the whole answer
and the point of streaming is gone. Vercel's docs say Python functions stream
by default, but the hosting decision for this project rests on that being true
through their proxy, not just inside my app, so the repo ships a probe instead
of a belief.

`/debug/stream` emits server-sent events at a cadence I choose, each stamped
with the time it left the server. `scripts/measure_stream.py` records when each
one arrives and compares the gaps. A streaming path shows arrival gaps near the
cadence. A buffering path shows one long wait and then a burst, and the script
exits non-zero.

| Where | Time to first event | Median arrival gap (asked for 200 ms) | Verdict |
|---|---|---|---|
| Local uvicorn | 67 ms | 201 ms | STREAMED |
| Local, from the browser probe on the front page | 39 ms | 202 ms | STREAMED |
| **Vercel deployment** | **445 ms** | **201 ms** | **STREAMED** |

The deployment answers the question v0 was built to ask: a Python function on
Vercel hands chunks over as they are produced, through their proxy, to a client
on the other side of the country. The 445 ms to the first event is my laptop's
distance to the region plus a cold start; the 201 ms median is the cadence the
server was asked for, arriving intact.

Reproduce: `python scripts/measure_stream.py <url>`.

Caveats, honestly:

- The in-process test suite **cannot** prove streaming. httpx's ASGI transport
  hands back the whole body at once, so the incremental check runs against
  uvicorn on a real socket (`tests/test_stream_live.py`) and against the
  deployed URL. The in-process tests only check the anti-buffering headers.
- When a caller hangs up, the gateway closes the provider connection. What the
  tests prove is that the request ends and the gateway stays healthy — not that
  the provider stops generating. Whether Groq actually stops billing a
  cancelled stream is measurable only against the real provider, and that check
  belongs with v1's token accounting.

## v0 — the proxy

`POST /v1/chat/completions` and `GET /v1/models` speak OpenAI's contract, so any
SDK reaches the gateway by changing `base_url` and nothing else. Two decisions
are worth naming:

**The upstream response is opened, its status read, and only then is a byte
given to the caller.** Failing over to a second provider is only honest before
the first token — after that the caller holds half an answer that a different
model cannot continue. v2 plugs into that gap; v0 just makes sure the gap
exists.

**The stream is relayed as raw bytes.** Parsing and re-encoding every frame
would add latency per token and give the gateway an opinion about a format the
provider owns.

| Behaviour | Where it's proven |
|---|---|
| Tokens reach the caller as the provider produces them | `test_streamed_tokens_arrive_as_they_are_produced` |
| Hanging up closes the upstream connection | `test_client_disconnect_does_not_wedge_the_gateway` |
| A provider 429 keeps its status and `retry-after` | `test_rate_limited_upstream_keeps_its_status_and_retry_after` |
| A non-JSON provider error still returns an OpenAI-shaped error | `test_non_json_upstream_error_is_still_an_openai_shaped_error` |
| A gateway with no keys refuses instead of standing open | `test_gateway_with_no_keys_refuses_instead_of_standing_open` |

Reproduce: `.venv/bin/python -m pytest -q` — 38 tests, no network, no provider
key. The provider in those tests is `tests/fake_upstream.py`, which rate-limits,
500s, stalls and dies mid-stream on request.

Caveats, honestly:

- **Keys live in an environment variable**, hashed with SHA-256 and compared in
  constant time, but not yet in Neon and not yet revocable without a redeploy.
- **The request log is a JSON line on stdout.** Vercel keeps an hour of runtime
  logs on the free plan, so usage history needs the database in v1.
- **Rate limiting arrived with v1, below**, but only its per-process baseline is
  measured so far.
- **Token counts come from the provider's final frame**, so a stream the caller
  abandons records no token usage even though the tokens were generated.

## v1 — a limit that survives a second copy (in progress)

Two ceilings are enforced per call, and both are decided in one operation: the
caller's own key, and the provider account everybody shares. Deciding them
separately would mean charging a caller for a request that the account budget
then refuses — and the refund path is exactly where a race lives.

Tokens, not just requests, because Groq's free tier binds there first. The cost
of a call is unknown until the reply exists, so the limiter reserves an
estimate (prompt characters ÷ 4, plus the completion the caller allowed) and
settles against the provider's real usage afterwards. An over-estimate is
refunded, an under-estimate is charged, and refunds clamp at the ceiling so a
run of cheap calls cannot mint budget.

The per-process limiter is kept deliberately, as the measured baseline:

40 requests, split evenly across two copies of the service, limit 10/minute:

| Buckets | Copies | Admitted | Over the limit |
|---|---|---|---|
| per-process | 2 | 20 (10 + 10) | **+100%** |
| shared, Upstash Redis | 2 | 10 (5 + 5) | **0%** |

That is the whole project in one table. The same code, the same burst, the same
limit; the only difference is where the bucket lives.

Reproduce: start two copies, then

```
python scripts/measure_limit.py --key qg_... --limit 10 --requests 40 \
  --url http://127.0.0.1:8201 --url http://127.0.0.1:8202
```

with `QUOTAGATE_REDIS_REST_URL`/`_TOKEN` set for the shared row and unset for the
per-process one. The Lua script itself is exercised against real Upstash by
`tests/test_live_services.py`.
The burst goes to `/debug/limit-check`, which takes the same limiter path as a
real call and stops before the provider, so measuring enforcement doesn't spend
the quota being enforced.

Decisions worth naming:

- **The whole check-and-deduct is one Lua script.** Read, decide, write in
  Python would let two copies read the same remaining budget and both spend it,
  which is the bug this repo exists to fix.
- **Unreachable limiter means refuse, not allow.** Refusing costs one caller a
  request; allowing spends a provider budget everyone shares. `QUOTAGATE_FAIL_OPEN`
  exists but has to be asked for.
- **`/healthz` says `shared` or `per-process`.** They are different promises, so
  a missing environment variable should not quietly downgrade one into the other.
- **Streaming requests get `stream_options.include_usage` added** if the caller
  didn't set it, otherwise every streamed call would settle against an estimate.

Caveats, honestly:

**What the limiter costs, measured where it runs.** Each call is one round trip
to Upstash, so the number depends entirely on who is asking:

| Asking from | Limiter round trip |
|---|---|
| my laptop | 131 ms |
| the deployment, first call on a cold instance | 43 ms |
| the deployment, warm | **4.3 ms** |

The gateway reports its own figure as `x-quotagate-limiter-ms` on every
response, because a number measured from my kitchen table is a measurement of
my broadband, not of the gateway.

In production, with the limit at 10 a minute, a burst of 30 requests was
admitted 9 times — the tenth had gone to a completion a moment earlier.
- **An abandoned stream keeps its full reservation** until it refills. Settling
  during teardown isn't reliable, so the error is toward under-serving rather
  than over-spending.
- **The estimate is crude** — four characters per token, no tokeniser. It is
  reconciled immediately afterwards, so the error window is one call wide.

## v2 — failing over without lying (in progress)

Providers are tried in order, and the decision to move on happens **before the
first byte reaches the caller**. After that it is too late: the caller holds
half an answer, and a second model cannot continue a sentence it did not start.
A stream that dies midway ends as a broken stream, without `[DONE]`, rather
than as a seam between two models pretending to be one.

What is worth moving on for, and what is not:

| Provider says | Gateway does | Why |
|---|---|---|
| unreachable / timed out | try the next provider | nothing was sent; nothing is lost |
| 5xx | try the next provider | the provider says it is broken |
| 429 | try the next provider | another provider may still have budget |
| 4xx | return it unchanged | the request is wrong; a second provider would reject it identically, and this is not the provider's fault |
| died mid-stream | end the stream | see above |

Two protections sit on top, and they protect the *provider* as much as the
caller — a gateway that retries everything doubles a provider's traffic exactly
when it is failing:

- **A circuit breaker per provider.** Five consecutive failures and it stops
  being asked for a cooldown; then exactly one call is let through to find out
  whether it recovered. Consecutive failures, not a failure rate, because a 10%
  error rate is a bad day rather than an outage.
- **A retry budget for the deployment.** Retries are capped as a fraction of
  traffic (one per five requests by default), not counted per request. With
  everything failing, 20 calls buy 8 retries, not 20.

Every response carries `x-quotagate-provider` and `x-quotagate-attempts`
(`alpha:500,beta:200`, or `alpha:skipped(circuit_open),beta:200`), so a failover
is visible from outside instead of only in logs.

| Injected fault | Result |
|---|---|
| Primary returns 500 | Second provider answers; `alpha:500,beta:200` |
| Primary rate limits (429) | Second provider answers |
| Primary unreachable | Second provider answers |
| Caller sends a bad request (400) | Returned as-is, no second attempt |
| Primary dies mid-stream | Stream ends broken, no retry, tokens already sent are kept |
| Primary fails 6 times | Circuit opens; later calls skip it entirely |
| Everything down, 12 calls | Retries stop when the budget runs out |
| **Groq unreachable, real call** | **OpenRouter answered: `groq:cannot reach groq,openrouter:200`** |

Reproduce: `.venv/bin/python -m pytest tests/test_failover.py tests/test_resilience.py -q`.
The providers are two copies of `tests/fake_upstream.py`; prefixing a model with
a provider name (`alpha:fake-500`) makes only that one misbehave.

Caveats, honestly:

- **The breaker and the budget are per-process.** For a breaker that is
  defensible — each copy learns from what it saw. For the budget it is not: N
  copies allow N times the retries. Both move into Redis once v1's shared
  buckets are verified against a real Redis.
- **Failover needs a model both providers serve.** `openai/gpt-oss-20b` exists
  on Groq and on OpenRouter, which is why the pair works; a Groq-only model id
  would fail over into a 404. Per-provider model mapping is not built yet, and
  the roadmap says so.
- **No hedging.** Sending the same request to two providers and taking the
  faster reply would cut tail latency and double the spend; on a free tier that
  trade is not available.

## v3 — answering twice for free (in progress)

A repeated question that pinned its sampling has exactly one right answer, so
the second caller gets the first caller's bytes. Only requests that declared
themselves deterministic qualify — `temperature: 0`, no tools, no `n` — because
anything else may legitimately differ next time.

| Request | Cached? |
|---|---|
| `temperature: 0` | yes |
| no `temperature` | no — that means the provider's default, not zero |
| `temperature: 0.9` | no |
| tools, `n > 1` | no — the reply can differ every time |

The key covers model, messages and every sampling parameter; anything left out
of a cache key is a way for two different questions to collide on one answer.
`stream` is deliberately not in the key — the same question has the same answer
whether it is streamed or buffered — but the two shapes are stored separately,
because handing a streaming SDK a buffered body gives it something it cannot
parse. Streamed replies are stored as the exact bytes that were relayed and
replayed as-is, so a cached stream and a live one are the same stream, faster.

**A hit refunds the token reservation**, because no provider call happened. The
request itself still counts against the caller's rate: a client looping on one
prompt is still traffic.

Every response says which it was: `x-quotagate-cache: hit | miss | skip`.

Reproduce: `.venv/bin/python -m pytest tests/test_cache.py -q`.

Caveats, honestly:

- **The cache is per-process and bounded** (256 entries, 5 minutes). On Vercel
  that means a hit needs the same instance to answer both calls, so the hit
  rate in production will be well below what a single laptop process suggests.
  The shared version lands with Redis.
- **No hit-rate number yet.** Measuring it honestly needs real repeated traffic,
  which arrives when BumpCheck starts calling the gateway.
- **Bodies over 256 KB stream to the caller but are not stored.**

## What's next

Verifying the Redis path against a real Redis, then deploying. See [ROADMAP.md](ROADMAP.md), including what I decided
not to build and why.

## Run locally

```
python3 -m venv .venv && .venv/bin/pip install -r requirements-dev.txt
.venv/bin/python -m pytest -q
.venv/bin/python -m uvicorn app:app --reload
```

## Layout

```
app.py                     Vercel entrypoint; exposes the ASGI app
quotagate/api.py           routes: front page, health, chat completions, probe
quotagate/upstream.py      provider calls, status-before-first-byte, relay
quotagate/keys.py          SHA-256 key digests, constant-time comparison
quotagate/requestlog.py    one JSON record per request
quotagate/page.py          front page; runs the streaming probe in the browser
scripts/measure_stream.py  streamed-or-buffered verdict for any deployment
scripts/new_key.py         mint a key, print its digest
scripts/measure_limit.py   burst across copies; admitted vs the limit
quotagate/limits.py        buckets, estimate, reconcile; per-process baseline
quotagate/redis_buckets.py the same buckets in one Lua script, two transports
quotagate/providers.py     provider order, and which failures are worth moving for
quotagate/resilience.py    circuit breaker, retry budget
docker-compose.yml         three copies + Redis, for the shared-limit test
tests/fake_upstream.py     a provider that 429s, 500s, stalls and dies on demand
tests/test_proxy.py        the proxy, over real sockets
tests/test_failover.py     two providers, injected faults, breaker, budget
tests/test_stream.py       in-process: headers, validation, event shape
tests/test_stream_live.py  real socket: arrival gaps, client disconnect
```
