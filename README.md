# quotagate

An OpenAI-compatible gateway that puts shared rate limits, failover and usage
accounting in front of a model provider.

Live: *not deployed yet — v0 lands this week.*

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

Not deployed yet. Locally:

```
.venv/bin/python scripts/new_key.py demo
QUOTAGATE_KEYS="demo:<digest>" QUOTAGATE_UPSTREAM_KEY="$GROQ_API_KEY" \
  .venv/bin/python -m uvicorn app:app
```

```
curl -N http://127.0.0.1:8000/v1/chat/completions \
  -H "authorization: Bearer qg_..." -H "content-type: application/json" \
  -d '{"model":"llama-3.3-70b-versatile","stream":true,
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
| Vercel deployment | *pending* | *pending* | *pending* |

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

Reproduce: `.venv/bin/python -m pytest -q` — 18 tests, no network, no provider
key. The provider in those tests is `tests/fake_upstream.py`, which rate-limits,
500s, stalls and dies mid-stream on request.

Caveats, honestly:

- **There is no rate limiting yet.** That is the entire point of the project and
  it lands in v1. Today a valid key can spend the whole Groq budget.
- **Keys live in an environment variable**, hashed with SHA-256 and compared in
  constant time, but not yet in Neon and not yet revocable without a redeploy.
- **The request log is a JSON line on stdout.** Vercel keeps an hour of runtime
  logs on the free plan, so usage history needs the database in v1.
- **Token counts come from the provider's final frame**, so a stream the caller
  abandons records no token usage even though the tokens were generated.

## What's next

v1 puts the token bucket in Upstash Redis and makes the limit hold across
copies of the service. See [ROADMAP.md](ROADMAP.md), including what I decided
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
tests/fake_upstream.py     a provider that 429s, 500s, stalls and dies on demand
tests/test_proxy.py        the proxy, over real sockets
tests/test_stream.py       in-process: headers, validation, event shape
tests/test_stream_live.py  real socket: arrival gaps, client disconnect
```
