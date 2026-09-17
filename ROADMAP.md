# Roadmap

Each milestone is deployed and measured before the next one starts. Numbers in
the README come from the deployed service, not from localhost.

## v0 — the streaming spine (in progress)

- [x] FastAPI app, health check, SSE probe endpoint with a known cadence
- [x] `scripts/measure_stream.py`: streamed-or-buffered verdict against any URL
- [x] Tests that a real socket, not an in-process transport, has to satisfy
- [ ] Deployed to Vercel, probe re-run against the deployment
- [x] `POST /v1/chat/completions` and `/v1/models`, streaming passed through
- [x] API keys as SHA-256 digests, compared in constant time
- [x] One JSON request record per call, written after the last byte
- [x] Front page that runs the probe in the visitor's browser
- [ ] Keys and request log in Neon (moves with v1's quota counters)
- [ ] Public demo key with a small quota (needs v1's limiter to be safe)

**Open question this milestone answers:** does a Python function on Vercel
deliver chunks as they are produced? Vercel's changelog says streaming is on by
default for Python. The probe is what makes that claim checkable, and the whole
hosting choice depends on it. If the deployed verdict comes back `BUFFERED`,
the gateway moves to Render and the Vercel project keeps only the front page.

## v1 — limits that hold across copies (in progress)

- [x] Token buckets with reserve-and-reconcile, per-key and per-account, decided
      together in one operation
- [x] Per-process baseline kept and measured: two copies admit twice the limit
- [x] `/debug/limit-check` so enforcement can be measured without spending quota
- [x] Fail-closed when the limiter is unreachable; `/healthz` reports which
      promise is in force
- [ ] The Lua script run against a real Redis, and the shared-limit row filled in
- [ ] Upstash account, then the same measurement against the deployment
- Token bucket in Upstash Redis, evaluated in one atomic script
- Per-key requests/minute and tokens/minute; reserve an estimate, reconcile
  against real usage from the response
- One shared Groq budget across every caller, because the free tier's
  8,000 tokens/minute belongs to the account, not to a request
- `RateLimit-*` and `Retry-After` headers
- A written decision on what happens when Redis is unreachable
- Cancel the upstream call when the client hangs up

**Measured:** admitted vs. allowed under a burst, in-memory vs. Redis, run
against three local copies and against the deployment; limiter latency cost;
behaviour with Redis switched off.

## v2 — failure is the normal case

- Second provider (OpenRouter), health-aware routing, failover before the first
  byte only — a half-sent stream cannot be retried onto another provider
- Circuit breaker state in Redis so every copy sees it
- Retry budget across requests instead of per-request retries
- Deadlines propagated to the upstream call
- A fake upstream that stalls, 500s, 429s and dies mid-stream

**Measured:** success rate and time-to-first-token under each injected fault,
with and without each mechanism.

## v3 — where the money and the errors went

- OpenTelemetry traces to Grafana Cloud: limiter, cache, provider, per request
- Per-key token and cost accounting, plus a reconciliation query that proves
  the rollups match the request log
- Exact-match response cache in Redis for deterministic requests
- A written SLO and a burn-rate alert
- BumpCheck's model calls move behind the gateway — its first real client

**Measured:** rollup drift against the log (target: zero rows), cache hit rate
on BumpCheck's real traffic, tracing overhead.

## Deliberately not planned

- **AWS, Kubernetes, Terraform.** Every one of them needs a card on file or a
  paid cluster, and this project is pinned at $0 forever. They are the largest
  gap between this repo and the job postings I read, and I would rather say
  that here than fake it with manifests that only ever run in CI.
- **Kafka.** There is no managed Kafka I can run without a card. The request
  log is a Postgres table; at demo volume that is the honest choice, and the
  README says what volume would change it.
- **Semantic caching.** Returning a neighbouring answer as if it were the
  answer is a correctness bug wearing a performance costume. Exact-match only.
- **Multi-region.** One region, stated plainly, beats a region map that has
  never served a request.
- **Prompt management, evals, agent orchestration.** BumpCheck already covers
  the model-facing side. This repo is the infrastructure in front of it.
- **My own load balancer.** Vercel decides how many copies run. That is what
  makes the Redis limiter necessary, and pretending otherwise would be theatre.
