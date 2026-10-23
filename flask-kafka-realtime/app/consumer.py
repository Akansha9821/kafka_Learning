
import json
import os
import signal
from confluent_kafka import Consumer

running = True


def stop_worker(signum, frame):
    global running
    running = False


signal.signal(signal.SIGTERM, stop_worker)
signal.signal(signal.SIGINT, stop_worker)

consumer = Consumer({
    "bootstrap.servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    ),
    "group.id": "order-processing-group",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
})

consumer.subscribe(["orders.created"])

print("Order consumer started. Waiting for events...", flush=True)

try:
    while running:
        message = consumer.poll(1.0)

        if message is None:
            continue

        if message.error():
            print(f"Kafka error: {message.error()}", flush=True)
            continue

        try:
            order = json.loads(message.value().decode("utf-8"))

            # Replace this with actual business processing.
            print(
                f"PROCESSED ORDER: {json.dumps(order)}",
                flush=True,
            )

            # Commit only after successful processing.
            consumer.commit(message=message, asynchronous=False)

        except Exception as exc:
            print(f"Processing failed: {exc}", flush=True)
            # Do not commit a failed event.
            # Add retry and dead-letter handling before production.
finally:
    consumer.close()
    print("Order consumer stopped.", flush=True)
