import logging
import time
from datetime import datetime, timezone

from .config import POLL_INTERVAL_SECONDS
from .db import Connection, IngestionCheckpoint, SessionLocal, init_db
from .ingest import parse_logs
from .portainer import PortainerClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("orc.worker")


def _poll_connection(conn_id: int, name: str, base_url: str, api_token: str) -> tuple[str, str | None]:
    client = PortainerClient(base_url, api_token)
    try:
        endpoints = client.get_endpoints()
    except Exception as exc:
        return "error", str(exc)

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
                                connection_id=conn_id,
                                endpoint_id=eid,
                                container_id=cid,
                                last_unix_ts=last_ts,
                            ))
                        session.commit()
                        log.info("[%s] %d event(s) from %s", name, len(events), cname)
                except Exception as exc:
                    log.error("[%s] failed to ingest %s: %s", name, cname, exc)

    return "ok", None


def _run_cycle() -> None:
    now = datetime.now(timezone.utc)
    with SessionLocal() as session:
        connections = [
            (c.id, c.name, c.base_url, c.api_token)
            for c in session.query(Connection).filter_by(enabled=True).all()
        ]

    if not connections:
        log.info("No enabled connections — skipping poll")
        return

    for conn_id, name, base_url, api_token in connections:
        status, error = _poll_connection(conn_id, name, base_url, api_token)
        with SessionLocal() as session:
            conn = session.get(Connection, conn_id)
            if conn:
                conn.last_polled_at = now
                conn.last_status = status
                conn.last_error = error
                session.commit()


def main() -> None:
    init_db()
    log.info("ORC worker started — poll interval %ds", POLL_INTERVAL_SECONDS)
    while True:
        try:
            _run_cycle()
        except Exception as exc:
            log.error("Poll cycle failed: %s", exc)
        time.sleep(POLL_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
