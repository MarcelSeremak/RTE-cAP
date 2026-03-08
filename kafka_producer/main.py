import os
from dotenv import load_dotenv
from .producer import Producer

load_dotenv()


def main():
    TOPIC = os.getenv("KAFKA_TOPIC")
    KAFKA_HOST = os.getenv("KAFKA_HOST", "kafka")
    KAFKA_PORT = os.getenv("KAFKA_INTERNAL_PORT", "19092")
    BOOTSTRAP = f"{KAFKA_HOST}:{KAFKA_PORT}"
    NAME = os.getenv("NAME")

    if not TOPIC:
        raise ValueError("Brakuje zmiennej środowiskowej TOPIC")

    producer = Producer(
        topic=TOPIC,
        bootstrap=BOOTSTRAP,
        name=NAME,
    )
    producer.run()


if __name__ == "__main__":
    main()