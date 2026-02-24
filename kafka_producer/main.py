from datetime import datetime
import time
import json
import random
import os
from prometheus_client import Counter, Gauge, start_http_server
from dotenv import load_dotenv
from kafka import KafkaProducer
from utils.logger import get_logger

logger = get_logger("kafka_producer")
load_dotenv()

TOPIC = os.getenv("KAFKA_TOPIC", "orders")
KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_INTERNAL_PORT", "19092")
METRICS_PORT = int(os.getenv("METRICS_PORT", "9103"))
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"

producer_sent_total = Counter("producer_sent_total", "Messages sent to Kafka")
producer_errors_total = Counter("producer_errors_total", "Kafka send errors")
producer_configured_rate = Gauge("producer_configured_rate", "Configured GEN_RATE per second")

def generate_order() -> dict:
    return {
        "order_id": random.randint(1, 9_999_999),
        "customer_id": random.randint(1, 200),
        "product_id": random.randint(1, 100),
        "quantity": random.randint(1, 5),
        "unit_price": round(random.uniform(10.0, 1000.0), 2),
        "currency": random.choice(["USD", "EUR", "GBP", "JPY"]),
        "timestamp": datetime.now().isoformat(),
    }

def get_rate() -> int:
    try:
        return max(int(os.getenv("GEN_RATE", "10")), 1)
    except ValueError:
        return 10

def main():
    start_http_server(METRICS_PORT)
    rate_per_sec = get_rate()
    sleep_time = 1.0 / rate_per_sec
    producer_configured_rate.set(rate_per_sec)

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        linger_ms=50,
    )
    cnt = 0
    logger.info(f"Producer started: topic={TOPIC} bootstrap={BOOTSTRAP} rate={rate_per_sec}/s")

    try:
        while True:
            order = generate_order()
            try:
                producer.send(TOPIC, key=str(order["customer_id"]), value=order)
                producer_sent_total.inc()
                cnt += 1
                if cnt % 100 == 0:
                    logger.info(f"Sent {cnt} messages to topic '{TOPIC}'")
            except Exception as e:
                producer_errors_total.inc()
                logger.warning(f"Send error: {repr(e)}")
            finally:
                time.sleep(sleep_time)
                
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        try:
            producer.flush()
            producer.close()
        except Exception as e:
            logger.warning(f"Producer close error: {repr(e)}")

if __name__ == "__main__":
    main()