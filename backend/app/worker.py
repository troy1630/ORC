"""
ORC Worker — polls one container at a time.

Interval: use the connection's poll_interval_seconds if set, otherwise
auto-compute as 100s / total-containers so a full cycle takes ~100 seconds.

Each Raven message includes both new events this cycle AND 24h recent counts
so the UI can show a container's true health state, not just latest activity.
"""

import logging
import time
from collections import deque
from datetime import datetime, timedelta, timezone

from sqlalchemy import func

from . import raven
from .config import POLL_INTERVAL_SECONDS
from .db import Connection, FocusedWatch, IngestionCheckpoint, ObservedEvent, SessionLocal, init_db
from .ingest import parse_logs
from .portainer import PortainerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orc.worker")

# Each entry: (conn_id, conn_name, server_display, endpoint_id, container_id, container_name, client, interval_s)
_queue: deque = deque()
_sleep: float = float(POLL_INTERVAL_SECONDS)
_warned_no_connections: bool = False


def _rebuild_queue() -> None:
    global _sleep, _warned_no_connections
    _queue.clear()
    now = datetime.now(timezone.utc)

    with SessionLocal() as s:
        connections = s.query(Connection).filter_by(enabled=True).all()
        conn_list = [
            (c.id, c.name, c.server_name or c.name, c.base_url, c.api_token, c.poll_interval_seconds)
            for c in connections
        ]

    if not conn_list:
        if not _warned_no_connections:
            log.info("No enabled connections — add one via the Connections tab")
            raven.publish({"type": "no_connections"})
            _warned_no_connections = True
        _sleep = 10.0
        return

    _warned_no_connections = False
    total = 0

    for conn_id, conn_name, server_display, base_url, api_token, conn_interval in conn_list:
        client = PortainerClient(base_url, api_token)
        conn_ok, conn_error = True, None
        try:
            endpoints = client.get_endpoints()
        except Exception as exc:
            conn_ok, conn_error = False, str(exc)
            log.error("[%s] cannot reach Portainer: %s", conn_name, exc)
            raven.publish({"type": "poll_error", "server": server_display, "server_key": conn_name, "error": conn_error})

        _update_conn(conn_id, "ok" if conn_ok else "error", conn_error, now)

        if not conn_ok:
            continue

        for ep in endpoints:
            eid = ep["Id"]
            try:
                for c in client.get_running_containers(eid):
                    cid = c["Id"]
                    cname = (c.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                    _queue.append((conn_id, conn_name, server_display, eid, cid, cname, client, conn_interval))
                    total += 1
            except Exception as exc:
                log.error("[%s] endpoint %s: %s", conn_name, eid, exc)

    if total:
        # Use per-connection interval if set, else auto-compute from queue size
        auto = round(100.0 / total, 2)
        _sleep = auto  # default; overridden per-item in _poll_next
        log.info("Queue: %d container(s), auto interval=%.2fs", total, auto)
        raven.publish({
            "type": "queue_ready",
            "containers": total,
            "connections": len(conn_list),
            "interval": auto,
        })
    else:
        _sleep = 10.0


def _recent_counts(conn_id: int, cid: str) -> tuple[int, int]:
    """Return (error_count, warning_count) for this container in the last 24h."""
    cutoff = datetime.now(timezone.utc) - timedelta(hours=24)
    with SessionLocal() as s:
        errors = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.connection_id == conn_id,
            ObservedEvent.container_id == cid,
            ObservedEvent.severity.in_(["error", "critical"]),
            ObservedEvent.occurred_at >= cutoff,
        ).scalar() or 0
        warnings = s.query(func.count(ObservedEvent.id)).filter(
            ObservedEvent.connection_id == conn_id,
            ObservedEvent.container_id == cid,
            ObservedEvent.severity == "warning",
            ObservedEvent.occurred_at >= cutoff,
        ).scalar() or 0
    return errors, warnings


def _poll_next() -> None:
    global _sleep
    conn_id, conn_name, server_display, eid, cid, cname, client, conn_interval = _queue.popleft()
    event_count = 0
    new_errors = 0
    new_warnings = 0
    issue_payloads: list[dict] = []

    # Use connection's configured interval, or keep current auto interval
    if conn_interval:
        _sleep = float(conn_interval)

    next_cname = _queue[0][5] if _queue else ""
    raven.publish({"type": "container_checking", "server": server_display, "server_key": conn_name, "container": cname, "next_container": next_cname})

    try:
        with SessionLocal() as session:
            checkpoint = (
                session.query(IngestionCheckpoint)
                .filter_by(connection_id=conn_id, endpoint_id=eid, container_id=cid)
                .first()
            )
            if not checkpoint:
                checkpoint = IngestionCheckpoint(
                    connection_id=conn_id, endpoint_id=eid,
                    container_id=cid, last_unix_ts=0, poll_count=0,
                )
                session.add(checkpoint)
                session.flush()

            since = checkpoint.last_unix_ts or 0
            raw = client.get_container_logs(eid, cid, since=since)
            events, last_ts = parse_logs(raw, conn_id, eid, cid, cname)
            event_count = len(events)
            new_errors = sum(1 for e in events if e.severity in ("error", "critical"))
            new_warnings = sum(1 for e in events if e.severity == "warning")
            checkpoint.poll_count = (checkpoint.poll_count or 0) + 1

            if events:
                session.add_all(events)
                session.flush()
                issue_payloads = raven.issue_event_payloads(server_display, events, server_key=conn_name)
                checkpoint.last_unix_ts = last_ts

            session.commit()

        recent_errors, recent_warnings = _recent_counts(conn_id, cid)

        for payload in issue_payloads:
            raven.publish(payload)

        raven.publish({
            "type": "container_result",
            "server": server_display,
            "server_key": conn_name,
            "container": cname,
            "events": event_count,
            "errors": new_errors,
            "warnings": new_warnings,
            "recent_errors": recent_errors,
            "recent_warnings": recent_warnings,
            "issue_events": len(issue_payloads),
        })

        if events:
            log.info("[%s] %s — %d events (%d errors)", conn_name, cname, len(events), new_errors)

    except Exception as exc:
        log.error("[%s] %s: %s", conn_name, cname, exc)
        recent_errors, recent_warnings = _recent_counts(conn_id, cid)
        raven.publish({
            "type": "container_result",
            "server": server_display,
            "server_key": conn_name,
            "container": cname,
            "events": 0,
            "errors": 0,
            "warnings": 0,
            "recent_errors": recent_errors,
            "recent_warnings": recent_warnings,
        })


def _poll_focused_watch(watch: FocusedWatch, conn: Connection) -> None:
    client = PortainerClient(conn.base_url, conn.api_token)
    event_count = 0
    new_errors = 0
    new_warnings = 0
    issue_payloads: list[dict] = []
    now = datetime.now(timezone.utc)
    try:
        with SessionLocal() as session:
            checkpoint = (
                session.query(IngestionCheckpoint)
                .filter_by(connection_id=watch.connection_id, endpoint_id=watch.endpoint_id, container_id=watch.container_id)
                .first()
            )
            if not checkpoint:
                checkpoint = IngestionCheckpoint(
                    connection_id=watch.connection_id,
                    endpoint_id=watch.endpoint_id,
                    container_id=watch.container_id,
                    last_unix_ts=0,
                    poll_count=0,
                )
                session.add(checkpoint)
                session.flush()

            since = checkpoint.last_unix_ts or 0
            raw = client.get_container_logs(watch.endpoint_id, watch.container_id, since=since)
            events, last_ts = parse_logs(raw, watch.connection_id, watch.endpoint_id, watch.container_id, watch.container_name)
            event_count = len(events)
            new_errors = sum(1 for e in events if e.severity in ("error", "critical"))
            new_warnings = sum(1 for e in events if e.severity == "warning")
            checkpoint.poll_count = (checkpoint.poll_count or 0) + 1

            if events:
                session.add_all(events)
                session.flush()
                issue_payloads = raven.issue_event_payloads(conn.server_name or conn.name, events, server_key=conn.name)
                checkpoint.last_unix_ts = last_ts

            watch_row = session.get(FocusedWatch, watch.id)
            if watch_row:
                watch_row.last_polled_at = now
            session.commit()

        recent_errors, recent_warnings = _recent_counts(watch.connection_id, watch.container_id)
        for payload in issue_payloads:
            payload["focused_watch"] = True
            raven.publish(payload)

        raven.publish({
            "type": "focused_watch_result",
            "server": conn.server_name or conn.name,
            "server_key": conn.name,
            "container": watch.container_name,
            "events": event_count,
            "errors": new_errors,
            "warnings": new_warnings,
            "recent_errors": recent_errors,
            "recent_warnings": recent_warnings,
            "watch_id": watch.id,
            "interval_seconds": watch.interval_seconds,
            "expires_at": watch.expires_at.isoformat(),
        })
    except Exception as exc:
        log.error("[focused-watch %s] %s: %s", watch.id, watch.container_name, exc)


def _run_focused_watches() -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        active = (
            s.query(FocusedWatch, Connection)
            .join(Connection, FocusedWatch.connection_id == Connection.id)
            .filter(Connection.enabled.is_(True), FocusedWatch.expires_at > now)
            .all()
        )

    for watch, conn in active:
        if watch.last_polled_at is None:
            due = True
        else:
            due = (now - watch.last_polled_at).total_seconds() >= max(1, watch.interval_seconds)
        if due:
            _poll_focused_watch(watch, conn)


def _cleanup_expired_focused_watches() -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        expired = s.query(FocusedWatch).filter(FocusedWatch.expires_at <= now).all()
        if not expired:
            return
        for row in expired:
            raven.publish({
                "type": "focused_watch_expired",
                "watch_id": row.id,
                "container": row.container_name,
                "connection_id": row.connection_id,
            })
            s.delete(row)
        s.commit()


def _next_focused_watch_sleep() -> float | None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        rows = s.query(FocusedWatch).filter(FocusedWatch.expires_at > now).all()
    if not rows:
        return None
    waits: list[float] = []
    for row in rows:
        if row.last_polled_at is None:
            waits.append(0.0)
            continue
        due_at = row.last_polled_at + timedelta(seconds=max(1, row.interval_seconds))
        waits.append(max(0.0, (due_at - now).total_seconds()))
    return min(waits) if waits else None


def _check_poll_interval_reverts() -> None:
    """Restore original poll intervals for connections whose temporary override has expired."""
    global _queue
    now = datetime.now(timezone.utc)
    with SessionLocal() as s:
        conns = (
            s.query(Connection)
            .filter(Connection.revert_at.isnot(None), Connection.revert_at <= now)
            .all()
        )
        if not conns:
            return
        for conn in conns:
            conn.poll_interval_seconds = conn.revert_poll_interval
            conn.revert_poll_interval = None
            conn.revert_at = None
            log.info("Reverted poll interval for connection %s (%s)", conn.id, conn.name)
        s.commit()
    _rebuild_queue()


def _update_conn(conn_id: int, status: str, error: str | None, ts: datetime) -> None:
    with SessionLocal() as s:
        c = s.get(Connection, conn_id)
        if c:
            c.last_polled_at = ts
            c.last_status = status
            c.last_error = error
            s.commit()


def main() -> None:
    init_db()
    log.info("ORC worker started")
    while True:
        try:
            _cleanup_expired_focused_watches()
            _check_poll_interval_reverts()
            if not _queue:
                _rebuild_queue()
            if _queue:
                _poll_next()
            _run_focused_watches()
        except Exception as exc:
            log.error("Worker error: %s", exc)
        next_focused = _next_focused_watch_sleep()
        wait_s = _sleep if next_focused is None else min(_sleep, max(1.0, next_focused))
        time.sleep(wait_s)


if __name__ == "__main__":
    main()
