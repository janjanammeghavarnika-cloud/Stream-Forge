from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime, timezone

config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": "streamforge-producer"
}

producer = Producer(config)

TOPIC = "truck-telemetry"
NUM_TRUCKS = 50


def delivery_report(err, msg):
    if err is not None:
        print(f"Delivery failed: {err}")
    else:
        print(
            f"Delivered: {msg.key().decode()} "
            f"-> {msg.topic()} [{msg.partition()}] "
            f"offset={msg.offset()}"
        )


try:
    for batch in range(5):
        for truck_number in range(1, NUM_TRUCKS + 1):

            telemetry = {
                "truck_id": f"TRUCK-{truck_number:05d}",
                "temperature": round(random.uniform(15.0, 45.0), 2),
                "timestamp": datetime.now(timezone.utc).isoformat()
            }

            producer.produce(
                topic=TOPIC,
                key=telemetry["truck_id"],
                value=json.dumps(telemetry),
                callback=delivery_report
            )

        producer.poll(0)
        producer.flush()

        print(f"Sent telemetry for {NUM_TRUCKS} trucks")

except KeyboardInterrupt:
    print("\nProducer stopped.")

finally:
    producer.flush()