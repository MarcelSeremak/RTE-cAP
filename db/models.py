from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy import BigInteger, Integer, Text, JSON, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import TIMESTAMP

class Base(DeclarativeBase):
    pass

class RawOrder(Base):
    __tablename__ = "raw_orders"
    __table_args__ = (
        UniqueConstraint("kafka_topic", "kafka_partition", "kafka_offset", name="uq_kafka_msg"),
        {"schema": "raw"})
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    kafka_topic: Mapped[str] = mapped_column(Text, nullable=False)
    kafka_partition: Mapped[int] = mapped_column(Integer, nullable=False)
    kafka_offset: Mapped[int] = mapped_column(BigInteger, nullable=False)
    kafka_key: Mapped[str | None] = mapped_column(Text, nullable=True)
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)
    event_ts: Mapped[object] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)
    ingested_at: Mapped[object] = mapped_column(TIMESTAMP(timezone=True), server_default=func.now(), nullable=False)