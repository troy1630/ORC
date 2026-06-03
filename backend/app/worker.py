import logging
import time
from datetime import datetime, timezone

from .config import CYCLE_SECONDS, POLL_INTERVAL_SECONDS
from .db import Connection, IngestionCheckpoint, SessionLocal, init_db
from . import hermes
from .ingest import parse_logs
from .portainer import PortainerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orc.worker")


def _pick_connection() -> tuple[int, str, str, str] | None:
    """Return the enabled connection polled least recently (or never)."""
    with SessionLocal() as s:
        conns = s.query(Connection).filter_by(enabled=True).all()
        if not conns:
            return None
        def _age(c: Connection) -> datetime:
            return c.last_polled_at or datetime.min.replace(tzinfo=timezone.utc)
        c = min(conns, key=_age)
        return (c.id, c.name, c.base_url, c.api_token)


def _poll_one(conn_id: int, name: str, base_url: str, api_token: str) -> tuple[str, str | None]:
    hermes.publish({"type": "poll_start", "server": name})

    client = PortainerClient(base_url, api_token)
    try:
        endpoints = client.get_endpoints()
    except Exception as exc:
        hermes.publish({"type": "poll_error", "server": name, "error": str(exc)})
        return "error", str(exc)

    total_events = 0
    total_containers = 0

    with SessionLocal() as session:
        for ep in endpoints:
            eid = ep["Id"]
            try:
                containers = client.get_containers(eid)
            except Exception as exc:
                log.error("[%s] endpoint %s: %s", name, eid, exc)
                continue

            for container in containers:
                cid = container["Id"]
                cname = (container.get("Names") or [f"/{cid[:12]}"])[0].lstrip("/")
                try:
                    checkpoint = (
                        session.query(IngestionCheckpoint)
                        .filter_by(connection_id=conn_id, endpoint_id=eid, container_id=cid)
                        .first()
                    )
                    since = checkpoint.last_unix_ts if checkpoint else 0
                    raw = client.get_container_logs(eid, cid, since=since)
                    events, last_ts = parse_logs(raw, conn_id, eid, cid, cname)

                    if events:
                        session.add_all(events)
                        if checkpoint:
                            checkpoint.last_unix_ts = last_ts
                        else:
                            session.add(IngestionCheckpoint(
                                connection_id=conn_id, endpoint_id=eid,
                                container_id=cid, last_unix_ts=last_ts,
                            ))
                        session.commit()

                    err_count = sum(1 for e in events if e.severity in ("error", "critical"))
                    warn_count = sum(1 for e in events if e.severity == "warning")

                    hermes.publish({
                        "type": "container_result",
                        "server": name,
                        "container": cname,
                        "events": len(events),
                        "errors": err_count,
                        "warnings": warn_count,
                    })

                    total_events += len(events)
                    total_containers += 1
                    log.info("[%s] %s — %d event(s), %d error(s)", name, cname, len(events), err_count)

                except Exception as exc:
                    log.error("[%s] failed to ingest %s: %s", name, cname, exc)

    hermes.publish({
        "type": "poll_complete",
        "server": name,
        "containers": total_containers,
        "total_events": total_events,
    })

    return "ok", None


def _run_cycle() -> None:
    conn = _pick_connection()
    if not conn:
        log.info("No enabled connections — skipping")
        return

    conn_id, name, base_url, api_token = conn
    now = datetime.now(timezone.utc)
    status, error = _poll_one(conn_id, name, base_url, api_token)

    with SessionLocal() as s:
        c = s.get(Connection, conn_id)
        if c:
            c.last_polled_at = now
            c.last_status = status
            c.last_error = error
            s.commit()


def main() -> None:
    init_db()
    log.info("ORC worker started — tick %ds, cycle target %ds", POLL_INTERVAL_SECONDS, CYCLE_SECONDS)
    while True:
        try:
            _run_cycle()
        except Exception as exc:
            log.error("Cycle failed: %s", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
