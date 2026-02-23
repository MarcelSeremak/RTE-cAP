import fastapi
import uvicorn
from utils.logger import get_logger
from sqlalchemy.exc import OperationalError
from sqlalchemy import text
from dotenv import load_dotenv
import os
from db.session import SessionLocal
from kafka import KafkaConsumer

app = fastapi.FastAPI(title="RTE-cAP API")
logger = get_logger("api")
load_dotenv()

KAFKA_HOST = os.getenv("KAFKA_HOST", "localhost")
KAFKA_PORT = os.getenv("KAFKA_HOST_PORT", "9092")
BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"

@app.get("/health")
def health_check():
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
        consumer.close()
        kafka_status = "healthy"
    except Exception as e:
        logger.error(f"Kafka connection error: {e}")
        kafka_status = "unhealthy"
    
    return {
        "status_api": "healthy",
        "status_db": db_status,
        "status_kafka": kafka_status
    }


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)