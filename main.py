
from quixstreams import Application
import json

def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG",
        consumer_group="weather_reader",
        auto_offset_reset="latest",
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(["Weather_data_demo"])

        while True:
            msg = consumer.poll(1)

            if msg is None:
                print("Waiting for messages...")
                continue

            if msg.error() is not None:
                raise Exception(msg.error())

            key = msg.key().decode("utf-8") if msg.key() else None
            value = json.loads(msg.value().decode("utf-8"))
            offset = msg.offset()

            print(f"Offset: {offset}, Key: {key}, Value: {value}")
            consumer.store_offsets(msg)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        pass