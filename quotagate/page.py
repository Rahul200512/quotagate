"""The front page.

It loads instantly and does something useful on its own: it runs the streaming
probe in the browser, so the first thing a visitor sees is evidence that this
deployment streams, measured on their connection rather than claimed in prose.
"""

from __future__ import annotations

INDEX_HTML = """<!doctype html>
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

  <h2>Point any OpenAI client at it</h2>
  <pre>curl -N $BASE/v1/chat/completions \\
  -H "authorization: Bearer $KEY" \\
  -H "content-type: application/json" \\
  -d '{"model":"llama-3.3-70b-versatile","stream":true,
       "messages":[{"role":"user","content":"hello"}]}'</pre>

  <footer>
    v0 &middot; <a href="https://github.com/Rahul200512/quotagate">source and roadmap</a>
    &middot; <a href="/docs">API docs</a>
  </footer>
</main>
<script>
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
        for (const line of decoder.decode(value).split("\\n")) {
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
