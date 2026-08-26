import json
import time
from datetime import datetime
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

trucks = [
    {"truck_id": "TRUCK_001", "temperature": 32.5},
    {"truck_id": "TRUCK_002", "temperature": 28.7},
    {"truck_id": "TRUCK_003", "temperature": 35.2},
]

print("StreamForge Telemetry Producer started...")

try:
    while True:
        for truck in trucks:
            data = {
                "truck_id": truck["truck_id"],
                "temperature": truck["temperature"],
                "timestamp": datetime.now().isoformat()
            }

            producer.produce(
                "test-topic",
                value=json.dumps(data)
            )

            producer.flush()

            print("Sent:", data)

        time.sleep(5)

except KeyboardInterrupt:
    print("\nProducer stopped.")