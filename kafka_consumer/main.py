import json
import os
from dotenv import load_dotenv
from kafka import KafkaConsumer
from prometheus_client import Counter, start_http_server
from sqlalchemy.exc import IntegrityError
from db.models import RawEvent
from db.session import SessionLocal
from utils.logger import get_logger

load_dotenv()
logger = get_logger("kafka_consumer")

TOPICS = ["orders", "payments", "shipments"]
GROUP_ID = os.getenv("KAFKA_GROUP_ID", "raw-events-writer")
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_INTERNAL_PORT", "19092")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"

consumer_written_total = Counter(
    "consumer_written_total",
    "Rows written to Postgres",
    ["topic"],
)

consumer_duplicates_total = Counter(
    "consumer_duplicates_total",
    "Duplicate Kafka events ignored",
    ["topic"],
)

consumer_errors_total = Counter(
    "consumer_errors_total",
    "Consumer DB errors",
    ["topic"],
)

consumer_bad_messages_total = Counter(
    "consumer_bad_messages_total",
    "Bad Kafka messages",
    ["topic"],
)


def parse_message(msg) -> dict | None:
    try:
        raw_text = msg.value.decode("utf-8")
        return json.loads(raw_text)
    except (UnicodeDecodeError, json.JSONDecodeError) as e:
        logger.warning(f"[BAD MSG] topic={msg.topic} offset={msg.offset}: {e}")
        return None


def main():
    start_http_server(METRICS_PORT)

    consumer = KafkaConsumer(
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )
    consumer.subscribe(TOPICS)

    logger.info(f"Consumer started: topics={TOPICS} -> Postgres(raw.raw_events)")

    processed = 0

    try:
        for msg in consumer:
            payload = parse_message(msg)

            if payload is None:
                consumer_bad_messages_total.labels(msg.topic).inc()
                consumer.commit()
                continue

            db = SessionLocal()
            try:
                row = RawEvent(
                    kafka_topic=msg.topic,
                    kafka_partition=msg.partition,
                    kafka_offset=msg.offset,
                    kafka_key=msg.key.decode("utf-8") if msg.key else None,
                    payload=payload,
                )

                db.add(row)
                db.commit()

                consumer_written_total.labels(msg.topic).inc()
                consumer.commit()

                processed += 1
                if processed % 100 == 0:
                    logger.info(f"Processed {processed} messages")

            except IntegrityError:
                db.rollback()
                consumer_duplicates_total.labels(msg.topic).inc()
                logger.info(f"[DUPLICATE] topic={msg.topic} offset={msg.offset}")
                consumer.commit()

            except Exception as e:
                db.rollback()
                consumer_errors_total.labels(msg.topic).inc()
                logger.error(f"[DB ERROR] topic={msg.topic} offset={msg.offset}: {repr(e)}")

            finally:
                db.close()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()