#!/usr/bin/env python3
"""Mint an API key and print what to configure.

    python scripts/new_key.py demo

Prints the key once — it is not stored anywhere in plaintext — and the
`name:digest` entry to append to QUOTAGATE_KEYS.
"""

from __future__ import annotations

import argparse
import secrets
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from quotagate.keys import digest  # noqa: E402  (after the path bootstrap)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("name", help="who the key is for, e.g. demo, bumpcheck")
    args = parser.parse_args()

    raw = f"qg_{secrets.token_urlsafe(24)}"
    print(f"key (shown once)   {raw}")
    print(f"QUOTAGATE_KEYS     {args.name}:{digest(raw)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
