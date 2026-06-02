from datetime import datetime, timezone

from sqlalchemy import BigInteger, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, sessionmaker

from .config import DATABASE_URL

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


class ObservedEvent(Base):
    __tablename__ = "observed_events"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    source: Mapped[str] = mapped_column(String(64), default="portainer")
    endpoint_id: Mapped[int] = mapped_column(Integer)
    container_id: Mapped[str] = mapped_column(String(64))
    container_name: Mapped[str] = mapped_column(String(256))
    stack_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    severity: Mapped[str] = mapped_column(String(16), default="info")
    message: Mapped[str] = mapped_column(Text)
    occurred_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    ingested_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        default=lambda: datetime.now(timezone.utc),
    )


class IngestionCheckpoint(Base):
    __tablename__ = "ingestion_checkpoints"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    endpoint_id: Mapped[int] = mapped_column(Integer)
    container_id: Mapped[str] = mapped_column(String(64))
    last_unix_ts: Mapped[int] = mapped_column(BigInteger, default=0)


def init_db() -> None:
    Base.metadata.create_all(bind=engine)
