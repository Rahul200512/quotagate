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

Nothing to try yet — v0 is mid-flight. The first thing this repo answers is
whether the platform I chose can stream at all:

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
| Vercel deployment | *pending* | *pending* | *pending* |

Reproduce: `python scripts/measure_stream.py <url>`.

Caveats, honestly:

- The in-process test suite **cannot** prove streaming. httpx's ASGI transport
  hands back the whole body at once, so the incremental check runs against
  uvicorn on a real socket (`tests/test_stream_live.py`) and against the
  deployed URL. The in-process tests only check the anti-buffering headers.
- v0 proves the *transport* streams. Cancelling the upstream model call when
  the client disconnects is v1's job; today the test only proves the request
  ends.

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
quotagate/api.py           routes: health, streaming probe
scripts/measure_stream.py  streamed-or-buffered verdict for any deployment
tests/test_stream.py       in-process: headers, validation, event shape
tests/test_stream_live.py  real socket: arrival gaps, client disconnect
```
