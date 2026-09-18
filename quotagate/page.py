"""The front page.

It loads instantly and does something useful on its own: it runs the streaming
probe in the browser, so the first thing a visitor sees is evidence that this
deployment streams, measured on their connection rather than claimed in prose.
"""

from __future__ import annotations

def render(demo_key: str) -> str:
    """The page, with the public demo key handed to its script."""
    return INDEX_HTML.replace("__DEMO_KEY__", demo_key)


INDEX_HTML = r"""<!doctype html>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>quotagate</title>
<style>
  :root {
    color-scheme: light dark;
    --bg: #fbfbfa; --fg: #1a1a18; --muted: #6b6b66;
    --line: #e2e2dd; --accent: #1f6f4a; --card: #ffffff;
  }
  @media (prefers-color-scheme: dark) {
    :root { --bg: #14140f; --fg: #ececEA; --muted: #9a9a92;
            --line: #2c2c26; --accent: #6fd39b; --card: #1b1b16; }
  }
  * { box-sizing: border-box; }
  body { margin: 0; padding: 48px 20px; background: var(--bg); color: var(--fg);
         font: 16px/1.6 ui-sans-serif, -apple-system, "Segoe UI", system-ui, sans-serif; }
  main { max-width: 680px; margin: 0 auto; }
  h1 { font-size: 1.6rem; margin: 0 0 4px; letter-spacing: -0.01em; }
  p.sub { color: var(--muted); margin: 0 0 32px; }
  h2 { font-size: 1rem; margin: 32px 0 8px; }
  pre { background: var(--card); border: 1px solid var(--line); border-radius: 8px;
        padding: 12px 14px; overflow-x: auto; font-size: 13px; }
  .probe { background: var(--card); border: 1px solid var(--line); border-radius: 8px; padding: 16px; }
  /* Model output is prose, not code: it has to wrap rather than scroll. */
  #answer { white-space: pre-wrap; overflow-wrap: anywhere; margin-top: 12px; }
  button { font: inherit; padding: 8px 14px; border-radius: 6px; border: 1px solid var(--line);
           background: var(--accent); color: #fff; cursor: pointer; }
  button[disabled] { opacity: .6; cursor: default; }
  table { border-collapse: collapse; width: 100%; margin-top: 12px; font-size: 14px; }
  th, td { text-align: left; padding: 4px 8px 4px 0; border-bottom: 1px solid var(--line); }
  .verdict { font-weight: 600; color: var(--accent); }
  a { color: var(--accent); }
  footer { margin-top: 40px; color: var(--muted); font-size: 14px; }
</style>
<main>
  <h1>quotagate</h1>
  <p class="sub">An OpenAI-compatible gateway: shared rate limits, failover and usage
  accounting in front of a model provider.</p>

  <h2>Does this deployment stream?</h2>
  <div class="probe">
    <p style="margin-top:0">The server sends 10 events, 200&nbsp;ms apart. If the platform
    buffered them, they would all land at once.</p>
    <button id="run">Run the probe</button>
    <div id="out"></div>
  </div>

  <h2>Ask the model through it</h2>
  <div class="probe">
    <p style="margin-top:0">This runs a real completion through the gateway: shared rate
    limit, provider failover, usage accounting. The demo key is public and deliberately
    small — 10 requests and 6,000 tokens a minute, shared by everyone on this page — so
    if you hold the button down you will meet the limiter, which is the point.</p>
    <button id="ask">Ask: “what is a rate limiter?”</button>
    <pre id="answer" hidden></pre>
    <div id="meta"></div>
  </div>

  <h2>Point any OpenAI client at it</h2>
  <pre id="snippet">curl -N https://quotagate.vercel.app/v1/chat/completions \\
  -H "authorization: Bearer DEMO_KEY" \\
  -H "content-type: application/json" \\
  -d '{"model":"openai/gpt-oss-20b","stream":true,
       "messages":[{"role":"user","content":"hello"}]}'</pre>

  <footer>
    v0 &middot; <a href="https://github.com/Rahul200512/quotagate">source and roadmap</a>
    &middot; <a href="/docs">API docs</a>
  </footer>
</main>
<script>
  const DEMO_KEY = "__DEMO_KEY__";
  if (!DEMO_KEY) {
    document.getElementById("ask").disabled = true;
    document.getElementById("meta").textContent =
      "No demo key is configured on this deployment.";
  }
  document.getElementById("snippet").textContent =
    document.getElementById("snippet").textContent.replace("DEMO_KEY", DEMO_KEY);

  const askButton = document.getElementById("ask");
  const answer = document.getElementById("answer");
  const meta = document.getElementById("meta");

  askButton.addEventListener("click", async () => {
    askButton.disabled = true;
    answer.hidden = false;
    answer.textContent = "";
    meta.innerHTML = "";
    const started = performance.now();
    let firstToken = null;

    const response = await fetch("/v1/chat/completions", {
      method: "POST",
      headers: { "content-type": "application/json", authorization: "Bearer " + DEMO_KEY },
      body: JSON.stringify({
        model: "openai/gpt-oss-20b",
        stream: true,
        max_tokens: 400,
        // gpt-oss thinks before it answers, and those reasoning tokens come
        // out of the same allowance. At 120 the whole budget went to thinking
        // and the demo rendered nothing.
        reasoning_effort: "low",
        messages: [{ role: "user", content: "In two sentences, what is a rate limiter?" }],
      }),
    });

    const info = {
      provider: response.headers.get("x-quotagate-provider"),
      attempts: response.headers.get("x-quotagate-attempts"),
      limiter: response.headers.get("x-quotagate-limiter-ms"),
      remaining: response.headers.get("ratelimit-remaining"),
      tokens: response.headers.get("ratelimit-tokens-remaining"),
    };

    if (response.status === 429) {
      const wait = response.headers.get("retry-after");
      answer.textContent =
        "Rate limited by " + (response.headers.get("ratelimit-bound-by") || "the gateway") +
        ". Try again in " + wait + "s — this is the limiter doing its job.";
      askButton.disabled = false;
      return;
    }
    if (!response.ok) {
      answer.textContent = "The gateway returned " + response.status + ".";
      askButton.disabled = false;
      return;
    }

    const reader = response.body.getReader();
    const decoder = new TextDecoder();
    let buffered = "";
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffered += decoder.decode(value, { stream: true });
      const frames = buffered.split("\n\n");
      buffered = frames.pop();
      for (const frame of frames) {
        if (!frame.startsWith("data: ") || frame.includes("[DONE]")) continue;
        try {
          const delta = JSON.parse(frame.slice(6)).choices?.[0]?.delta || {};
          const text = delta.content || "";
          if (text) {
            if (firstToken === null) firstToken = performance.now() - started;
            answer.textContent += text;
          }
        } catch (err) { /* a frame split across reads */ }
      }
    }

    meta.innerHTML =
      "<table>" +
      "<tr><th>answered by</th><td>" + info.provider + " (" + info.attempts + ")</td></tr>" +
      "<tr><th>first token</th><td>" + (firstToken ? firstToken.toFixed(0) : "—") + " ms</td></tr>" +
      "<tr><th>limiter cost</th><td>" + info.limiter + " ms</td></tr>" +
      "<tr><th>your remaining budget</th><td>" + info.remaining + " requests, " +
      info.tokens + " tokens this minute</td></tr></table>";
    askButton.disabled = false;
  });

  const button = document.getElementById("run");
  const out = document.getElementById("out");
  button.addEventListener("click", async () => {
    button.disabled = true;
    out.innerHTML = "<p>listening…</p>";
    const started = performance.now();
    const arrivals = [];
    try {
      const response = await fetch("/debug/stream?chunks=10&gap_ms=200");
      const reader = response.body.getReader();
      const decoder = new TextDecoder();
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        for (const line of decoder.decode(value).split("\n")) {
          if (line.startsWith("data: ") && !line.includes("[DONE]")) {
            arrivals.push(performance.now() - started);
          }
        }
      }
    } catch (err) {
      out.innerHTML = "<p>probe failed: " + err + "</p>";
      button.disabled = false;
      return;
    }
    const gaps = arrivals.slice(1).map((t, i) => t - arrivals[i]).sort((a, b) => a - b);
    const median = gaps.length ? gaps[Math.floor(gaps.length / 2)] : 0;
    const streamed = median >= 100;
    out.innerHTML =
      "<table>" +
      "<tr><th>events</th><td>" + arrivals.length + "</td></tr>" +
      "<tr><th>first event</th><td>" + arrivals[0].toFixed(0) + " ms</td></tr>" +
      "<tr><th>median gap</th><td>" + median.toFixed(0) + " ms (asked for 200)</td></tr>" +
      "<tr><th>verdict</th><td class='verdict'>" +
      (streamed ? "STREAMED" : "BUFFERED") + "</td></tr></table>";
    button.disabled = false;
  });
</script>
"""
