import re
from datetime import datetime, timezone

from .db import ObservedEvent

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})\.(\d+)Z\s*")
_SEVERITY: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\b(CRITICAL|FATAL)\b", re.I), "critical"),
    (re.compile(r"\bERROR\b", re.I), "error"),
    (re.compile(r"\b(WARNING|WARN)\b", re.I), "warning"),
]


def _detect_severity(msg: str) -> str:
    for pattern, level in _SEVERITY:
        if pattern.search(msg):
            return level
    return "info"


def parse_logs(
    raw: str,
    connection_id: int,
    endpoint_id: int,
    container_id: str,
    container_name: str,
) -> tuple[list[ObservedEvent], int]:
    """Parse raw Docker log text into ObservedEvent rows. Returns (events, last_unix_ts)."""
    events: list[ObservedEvent] = []
    last_ts = 0

    for line in raw.splitlines():
        line = _ANSI_RE.sub("", line).strip()
        if not line:
            continue

        m = _TS_RE.match(line)
        if m:
            frac = m.group(2)[:6].ljust(6, "0")
            message = line[m.end():]
            try:
                occurred_at = datetime.fromisoformat(f"{m.group(1)}.{frac}+00:00")
            except ValueError:
                occurred_at = datetime.now(timezone.utc)
        else:
            message = line
            occurred_at = datetime.now(timezone.utc)

        last_ts = max(last_ts, int(occurred_at.timestamp()))
        events.append(
            ObservedEvent(
                connection_id=connection_id,
                endpoint_id=endpoint_id,
                container_id=container_id,
                container_name=container_name,
                severity=_detect_severity(message),
                message=message[:2048],
                occurred_at=occurred_at,
                ingested_at=datetime.now(timezone.utc),
            )
        )

    return events, last_ts
