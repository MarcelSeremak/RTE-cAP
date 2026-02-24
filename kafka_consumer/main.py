import json
from kafka import KafkaConsumer
from pydantic import ValidationError
from dotenv import load_dotenv
from prometheus_client import Counter, start_http_server
from utils.logger import get_logger
from db.session import SessionLocal
from db.models import RawOrder
from models.orders import OrderEvent
import os

logger = get_logger("kafka_consumer")
load_dotenv()

TOPIC = "orders"
GROUP_ID = "raw-orders-writer"
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_INTERNAL_PORT", "19092")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9102"))
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"

consumer_written_total = Counter("consumer_written_total", "Rows written to Postgres")
consumer_errors_total = Counter("consumer_errors_total", "Consumer errors")


def parse_message(msg) -> OrderEvent | None:
    try:
        raw_text = msg.value.decode("utf-8")
    except UnicodeDecodeError as e:
        logger.warning(f"[BAD ENCODING] offset={msg.offset}: {e}")
        return None

    try:
        data = json.loads(raw_text)
    except json.JSONDecodeError as e:
        logger.warning(f"[BAD JSON] offset={msg.offset}: {e}")
        return None

    try:
        return OrderEvent(**data)
    except ValidationError as e:
        logger.warning(f"[VALIDATION ERROR] offset={msg.offset}: {e.errors()}")
        return None


def main():
    start_http_server(METRICS_PORT)
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP,
        group_id=GROUP_ID,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
    )

    logger.info(f"Consumer started: {TOPIC} -> Postgres(raw.raw_orders)")
    cnt = 0 

    try:
        for msg in consumer:
            event = parse_message(msg)

            if event is None:
                consumer.commit()
                continue

            db = SessionLocal()
            try:
                row = RawOrder(
                    kafka_topic=msg.topic,
                    kafka_partition=msg.partition,
                    kafka_offset=msg.offset,
                    kafka_key=msg.key.decode("utf-8") if msg.key else None,
                    payload=event.model_dump(mode="json"),
                )
                cnt += 1
                if cnt % 100 == 0:
                    logger.info(f"Processed {cnt} messages")
                db.add(row)
                db.commit()
                consumer_written_total.inc()
                consumer.commit()

            except Exception as e:
                db.rollback()
                consumer_errors_total.inc()
                logger.error(f"[DB ERROR] offset={msg.offset}: {repr(e)}")
            finally:
                db.close()

    except KeyboardInterrupt:
        logger.info("Stopping consumer...")
    finally:
        consumer.close()


if __name__ == "__main__":
    main()