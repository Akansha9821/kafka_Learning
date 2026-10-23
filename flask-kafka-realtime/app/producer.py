
import json
import os
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": os.getenv(
        "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"
    ),
    "client.id": "flask-order-api",
    "acks": "all",
})


def publish_order(order):
    delivered = []

    def delivery_report(err, message):
        if err:
            delivered.append(err)
        else:
            delivered.append(None)

    producer.produce(
        topic="orders.created",
        key=order["order_id"],
        value=json.dumps(order),
        callback=delivery_report,
    )

    # Wait for the delivery callback before acknowledging success.
    remaining = producer.flush(5)

    if remaining > 0:
        raise TimeoutError("Kafka delivery timed out")

    if not delivered or delivered[-1] is not None:
        raise RuntimeError(
            f"Kafka delivery failed: {delivered[-1] if delivered else 'unknown error'}"
        )
