"""
Hermes — ORC's internal messaging bus.

The worker publishes structured activity messages to a Redis channel.
The API streams them to the browser via SSE so the UI updates in real time.
"""

import json
from datetime import datetime, timezone
from typing import Any

import redis as _redis

from .config import REDIS_URL

CHANNEL = "hermes"

_client: _redis.Redis | None = None


def _get() -> _redis.Redis:
    global _client
    if _client is None:
        _client = _redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _client


def publish(msg: dict[str, Any]) -> None:
    """Publish a message to the Hermes channel. Silently drops if Redis is unavailable."""
    msg.setdefault("ts", datetime.now(timezone.utc).isoformat())
    try:
        _get().publish(CHANNEL, json.dumps(msg))
    except Exception:
        pass
