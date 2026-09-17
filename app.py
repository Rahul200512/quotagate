"""Vercel entrypoint. Vercel loads the top-level `app` from this file."""

from quotagate.api import app

__all__ = ["app"]
