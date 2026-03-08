from prometheus_client import Counter, Gauge, start_http_server
import json
from kafka import KafkaProducer
from redis import Redis
from datetime import datetime
import time
from dotenv import load_dotenv
import random
from utils.logger import get_logger
import os

load_dotenv()

class Producer():

    def __init__(self, topic: str, bootstrap: str, name: str):
        self.topic = topic
        self.bootstrap = bootstrap
        self.logger = get_logger(f"kafka_producer_{name.replace(' ', '_')}")
        self.producer_sent_total = Counter("producer_sent_total", "Messages sent to Kafka")
        self.producer_errors_total = Counter("producer_errors_total", "Kafka send errors")
        self.producer_configured_rate = Gauge("producer_configured_rate", "Configured GEN_RATE per second")
        self.redis = Producer.get_redis_client()

    @staticmethod
    def get_redis_client() -> Redis:
        url = os.getenv("REDIS_URL")
        return Redis.from_url(url, decode_responses=True)

    @staticmethod
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

    @staticmethod
    def generate_payment(order_id: int, customer_id: int, amount: float, currency: str) -> dict:
        return {
            "payment_id": random.randint(1, 9_999_999),
            "order_id": order_id,
            "customer_id": customer_id,
            "amount": amount,
            "currency": currency,
            "payment_date": datetime.now().isoformat(),
        }
    
    @staticmethod
    def generate_shipment(order_id: int, payment_id: int, customer_id: int) -> dict:
        return {
            "shipment_id": random.randint(1, 9_999_999),
            "payment_id": payment_id,
            "order_id": order_id,
            "customer_id": customer_id,
            "ship_date": datetime.now().isoformat(),
        }
    
    @staticmethod
    def get_rate() -> int:
        try:
            return max(int(os.getenv("GEN_RATE")), 1)
        except ValueError:
            return 10
        
    def run(self):
        rate_per_sec = self.get_rate()
        sleep_time = 1.0 / rate_per_sec
        self.producer_configured_rate.set(rate_per_sec)
        start_http_server(int(os.getenv("METRICS_PORT")))

        producer = KafkaProducer(
            bootstrap_servers=self.bootstrap,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            linger_ms=50,
        )
        cnt = 0
        self.logger.info(f"Producer started: topic={self.topic} bootstrap={self.bootstrap} rate={rate_per_sec}/s")
        event_generator = {
                    "orders": Producer.generate_order,
                    "shipments": Producer.generate_shipment,
                    "payments": Producer.generate_payment,
                }
        generator = event_generator.get(self.topic)
        if generator is None:
            raise ValueError("Invalid topic name")
        try:
            while True:
                event = None
                try:
                    if self.topic == "orders":
                        event = generator()
                        order_data = {
                            "order_id": event["order_id"],
                            "customer_id": event["customer_id"],
                            "amount": event["quantity"] * event["unit_price"],
                            "currency": event["currency"],
                        }
                    elif self.topic == "payments":
                        _, payload = self.redis.blpop("orders", timeout=0)
                        if payload is None:
                            continue

                        order_data = json.loads(payload)
                        event = generator(
                            order_data["order_id"],
                            order_data["customer_id"],
                            order_data["amount"],
                            order_data["currency"],
                        )

                        payment_data = {
                            "payment_id": event["payment_id"],
                            "order_id": event["order_id"],
                            "customer_id": event["customer_id"],
                        }
                    elif self.topic == "shipments":
                        _, payload = self.redis.blpop("payments", timeout=0)
                        if payload is None:
                            continue

                        payment_data = json.loads(payload)
                        event = generator(
                            payment_data["order_id"],
                            payment_data["payment_id"],
                            payment_data["customer_id"],
                        )

                except Exception as e:
                    self.logger.error(f"Error generating event: {e}")
                    time.sleep(sleep_time)
                    continue

                try:
                    check = producer.send(self.topic, key=str(event["order_id"]), value=event)
                    check.get(timeout=5)

                    if self.topic == "orders":
                        self.redis.rpush("orders", json.dumps(order_data))
                    elif self.topic == "payments":
                        self.redis.rpush("payments", json.dumps(payment_data))
                    
                    self.producer_sent_total.inc()
                    cnt += 1
                    if cnt % 100 == 0:
                        self.logger.info(f"Sent {cnt} messages to topic '{self.topic}'")
                except Exception as e:
                    self.logger.error(f"Error sending message to Kafka: {e}")
                    self.producer_errors_total.inc()
                finally:
                    time.sleep(sleep_time)
        except KeyboardInterrupt:
            self.logger.info("Producer shutting down...")
        finally:
            producer.flush()
            producer.close()
        