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
    poll_count: Mapped[int] = mapped_column(BigInteger, default=0)


class AgentRuntimeState(Base):
    __tablename__ = "agent_runtime_states"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    agent_id: Mapped[str] = mapped_column(String(128), unique=True)
    name: Mapped[str] = mapped_column(String(128))
    role: Mapped[str] = mapped_column(String(128), default="specialist")
    icon: Mapped[str] = mapped_column(String(512), default="")
    trust_mode: Mapped[str] = mapped_column(String(32), default="recommend_only")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class AgentMessage(Base):
    __tablename__ = "agent_messages"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    thread_id: Mapped[str] = mapped_column(String(128), default="operations")
    source_agent: Mapped[str] = mapped_column(String(128))
    target_agent: Mapped[str | None] = mapped_column(String(128), nullable=True)
    message_type: Mapped[str] = mapped_column(String(64), default="status")
    summary: Mapped[str] = mapped_column(Text)
    payload: Mapped[str | None] = mapped_column(Text, nullable=True)
    visibility: Mapped[str] = mapped_column(String(32), default="operator")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class ApprovalRequest(Base):
    __tablename__ = "approval_requests"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(256))
    requester_agent: Mapped[str] = mapped_column(String(128), default="oracle")
    approver_agent: Mapped[str] = mapped_column(String(128), default="gate-keeper")
    action_type: Mapped[str] = mapped_column(String(128), default="container_refresh")
    target: Mapped[str] = mapped_column(String(256), default="")
    rationale: Mapped[str] = mapped_column(Text, default="")
    risk_level: Mapped[str] = mapped_column(String(32), default="medium")
    status: Mapped[str] = mapped_column(String(32), default="pending")
    requested_by: Mapped[str] = mapped_column(String(128), default="operator")
    decided_by: Mapped[str | None] = mapped_column(String(128), nullable=True)
    decision_reason: Mapped[str | None] = mapped_column(Text, nullable=True)
    execution_allowed: Mapped[bool] = mapped_column(Boolean, default=False)
    requested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    decided_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)


class LearningEntry(Base):
    __tablename__ = "learning_entries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(256))
    source_agent: Mapped[str] = mapped_column(String(128), default="sage")
    incident_ref: Mapped[str] = mapped_column(String(256), default="")
    outcome: Mapped[str] = mapped_column(String(64), default="proposed")
    summary: Mapped[str] = mapped_column(Text)
    markdown_path: Mapped[str] = mapped_column(String(512), default="")
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class UserAccount(Base):
    __tablename__ = "user_accounts"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    username: Mapped[str] = mapped_column(String(128), unique=True)
    password_hash: Mapped[str] = mapped_column(Text)
    role: Mapped[str] = mapped_column(String(32), default="user")
    enabled: Mapped[bool] = mapped_column(Boolean, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )


class UserSession(Base):
    __tablename__ = "user_sessions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    token_hash: Mapped[str] = mapped_column(String(128), unique=True)
    user_id: Mapped[int] = mapped_column(Integer, ForeignKey("user_accounts.id", ondelete="CASCADE"))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=lambda: datetime.now(timezone.utc)
    )
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))


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
        "ALTER TABLE ingestion_checkpoints ADD COLUMN IF NOT EXISTS poll_count BIGINT NOT NULL DEFAULT 0",
        "ALTER TABLE connections ADD COLUMN IF NOT EXISTS poll_interval_seconds INTEGER",
        "ALTER TABLE connections ADD COLUMN IF NOT EXISTS server_name VARCHAR(128)",
        "ALTER TABLE connections ADD COLUMN IF NOT EXISTS logo_data TEXT",
        "ALTER TABLE agent_runtime_states ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP WITH TIME ZONE",
        "ALTER TABLE approval_requests ADD COLUMN IF NOT EXISTS execution_allowed BOOLEAN NOT NULL DEFAULT FALSE",
        "ALTER TABLE user_accounts ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE",
    ]
    with engine.begin() as conn:
        for sql in stmts:
            try:
                conn.execute(text(sql))
            except Exception:
                pass
