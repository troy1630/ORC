from datetime import datetime, timezone

from sqlalchemy import (
    BigInteger, Boolean, DateTime, ForeignKey, Integer, String, Text,
    create_engine, text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from .config import DATABASE_URL

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class Connection(Base):
    __tablename__ = "connections"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String(128))
    type: Mapped[str] = mapped_column(String(32), default="portainer")
    base_url: Mapped[str] = mapped_column(String(512))
    api_token: Mapped[str] = mapped_column(Text)
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    poll_interval_seconds: Mapped[int | None] = mapped_column(Integer, nullable=True)
    server_name: Mapped[str | None] = mapped_column(String(128), nullable=True)
    logo_data: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    last_polled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_status: Mapped[str | None] = mapped_column(String(32), nullable=True)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)


class ObservedEvent(Base):
    __tablename__ = "observed_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    connection_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("connections.id", ondelete="SET NULL"), nullable=True
    )
    source: Mapped[str] = mapped_column(String(64), default="portainer")
    endpoint_id: Mapped[int] = mapped_column(Integer)
    container_id: Mapped[str] = mapped_column(String(64))
    container_name: Mapped[str] = mapped_column(String(256))
    stack_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    severity: Mapped[str] = mapped_column(String(16), default="info")
    message: Mapped[str] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class IngestionCheckpoint(Base):
    __tablename__ = "ingestion_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    connection_id: Mapped[int] = mapped_column(Integer, default=0)
    endpoint_id: Mapped[int] = mapped_column(Integer)
    container_id: Mapped[str] = mapped_column(String(64))
    last_unix_ts: Mapped[int] = mapped_column(BigInteger, default=0)


def init_db(retries: int = 10, delay: float = 3.0) -> None:
    import logging
    import time

    log = logging.getLogger("orc.db")
    for attempt in range(1, retries + 1):
        try:
            Base.metadata.create_all(bind=engine)
            _migrate()
            return
        except Exception as exc:
            if attempt == retries:
                raise
            log.warning("DB not ready (attempt %d/%d): %s — retrying in %.0fs", attempt, retries, exc, delay)
            time.sleep(delay)


def _migrate() -> None:
    """Add columns introduced after initial schema creation."""
    stmts = [
        "ALTER TABLE observed_events ADD COLUMN IF NOT EXISTS connection_id INTEGER REFERENCES connections(id) ON DELETE SET NULL",
        "ALTER TABLE ingestion_checkpoints ADD COLUMN IF NOT EXISTS connection_id INTEGER NOT NULL DEFAULT 0",
        "ALTER TABLE connections ADD COLUMN IF NOT EXISTS poll_interval_seconds INTEGER",
        "ALTER TABLE connections ADD COLUMN IF NOT EXISTS server_name VARCHAR(128)",
        "ALTER TABLE connections ADD COLUMN IF NOT EXISTS logo_data TEXT",
    ]
    with engine.begin() as conn:
        for sql in stmts:
            try:
                conn.execute(text(sql))
            except Exception:
                pass
