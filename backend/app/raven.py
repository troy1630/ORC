"""
Raven — ORC's internal messaging bus.

The worker publishes structured activity messages to a Redis channel.
The API streams them to the browser via SSE so the UI updates in real time.
"""

import json
from datetime import datetime, timezone
from typing import Any

import redis as _redis

from .config import REDIS_URL

CHANNEL = "raven"
ISSUE_SEVERITIES = {"warning", "error", "critical"}

_client: _redis.Redis | None = None


def _get() -> _redis.Redis:
    global _client
    if _client is None:
        _client = _redis.Redis.from_url(REDIS_URL, decode_responses=True)
    return _client


def publish(msg: dict[str, Any]) -> None:
    """Publish a message to the Raven channel. Silently drops if Redis is unavailable."""
    msg.setdefault("ts", datetime.now(timezone.utc).isoformat())
    try:
        _get().publish(CHANNEL, json.dumps(msg))
    except Exception:
        pass


def _iso(value: Any) -> str | None:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        return value.isoformat()
    return str(value)


def issue_event_payload(server: str, event: Any, server_key: str | None = None) -> dict[str, Any] | None:
    severity = str(getattr(event, "severity", "") or "").lower()
    if severity not in ISSUE_SEVERITIES:
        return None

    payload: dict[str, Any] = {
        "type": "issue_event",
        "server": server,
        "container": getattr(event, "container_name", ""),
        "severity": severity,
        "message": getattr(event, "message", ""),
        "occurred_at": _iso(getattr(event, "occurred_at", None)),
    }
    if server_key:
        payload["server_key"] = server_key
    event_id = getattr(event, "id", None)
    if event_id is not None:
        payload["event_id"] = event_id
    return payload


def issue_event_payloads(
    server: str, events: list[Any], limit: int = 25, server_key: str | None = None
) -> list[dict[str, Any]]:
    payloads: list[dict[str, Any]] = []
    for event in events:
        payload = issue_event_payload(server, event, server_key=server_key)
        if payload:
            payloads.append(payload)
        if len(payloads) >= limit:
            break
    return payloads
