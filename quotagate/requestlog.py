"""What happened on each request.

v0 writes one JSON line per request to stdout, which Vercel's runtime logs
collect. v1 writes the same record to Neon so usage can be summed and
reconciled. The record is defined here, once, so that move changes the sink and
not the call sites.

Nothing is written while the caller is waiting: for a streamed response the
record is completed after the last byte has already left, and for a buffered
one after the response is built.
"""

from __future__ import annotations

import json
import sys
import time
from dataclasses import asdict, dataclass, field


@dataclass
class RequestRecord:
    key_name: str
    model: str
    streamed: bool
    upstream: str
    status: int = 0
    # Outcome is the field I actually want when reading logs later: "ok",
    # "upstream_error", "client_disconnect", "gateway_error".
    outcome: str = "ok"
    upstream_ms: float = 0.0
    first_byte_ms: float | None = None
    chunks: int = 0
    bytes_out: int = 0
    prompt_tokens: int | None = None
    completion_tokens: int | None = None
    started_at: float = field(default_factory=time.time)

    def finish(self, status: int, outcome: str, started: float) -> None:
        self.status = status
        self.outcome = outcome
        self.upstream_ms = round((time.perf_counter() - started) * 1000, 1)

    def emit(self) -> None:
        payload = asdict(self)
        payload["started_at"] = round(self.started_at, 3)
        print(json.dumps(payload, separators=(",", ":")), file=sys.stdout, flush=True)
