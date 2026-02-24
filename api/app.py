import fastapi
from fastapi import Request, Response
import uvicorn
from utils.logger import get_logger
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from dotenv import load_dotenv
import os
from db.session import SessionLocal
from kafka import KafkaConsumer
from prometheus_client import generate_latest, Gauge, Counter, CONTENT_TYPE_LATEST

api_requests_total = Counter("api_requests_total", "HTTP requests", ["path", "status"])
api_db_healthy = Gauge("api_db_healthy", "DB health 1/0")
api_kafka_healthy = Gauge("api_kafka_healthy", "Kafka health 1/0")
kafka_brokers = Gauge("kafka_brokers", "Number of brokers")
kafka_topics = Gauge("kafka_topics", "Number of topics")

app = fastapi.FastAPI(title="RTE-cAP API")
logger = get_logger("api")
load_dotenv()

KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
KAFKA_PORT = os.getenv("KAFKA_INTERNAL_PORT", "19092")
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"

@app.middleware("http")
async def count_requests(request: Request, call_next):
    resp = await call_next(request)
    path = request.url.path
    api_requests_total.labels(path, str(resp.status_code)).inc()
    return resp

@app.get("/health")
def health_check():
    logger.info("Performing health check")
    return {
        "status_api": "healthy",
        "code": 200
    }

@app.get("/metrics")
def metrics():
    logger.info("Collecting metrics")
    db = SessionLocal()
    try:
        db.execute(text("SELECT 1"))
        db_status = "healthy"
    except OperationalError as e:
        logger.error(f"Database connection error: {e}")
        db_status = "unhealthy"
    finally:
        db.close()
    try:
        consumer = KafkaConsumer(bootstrap_servers=BOOTSTRAP, api_version_auto_timeout_ms=2000)
        topics = consumer.topics()
        kafka_topics.set(len(topics))
        brokers = list(consumer._client.cluster.brokers())
        kafka_brokers.set(len(brokers))
        consumer.close()
        kafka_status = "healthy"
    except Exception as e:
        logger.error(f"Kafka connection error: {e}")
        kafka_status = "unhealthy"
    finally:
        consumer.close()
    
    api_db_healthy.set(1 if db_status == "healthy" else 0)
    api_kafka_healthy.set(1 if kafka_status == "healthy" else 0)
    
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)