import json
from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-workers-v4",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["test-topic"])

truck_state = {}

print("StreamForge Worker started...")
print("Live fleet monitoring active...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))

            truck_id = data["truck_id"]
            temperature = data["temperature"]
            timestamp = data["timestamp"]

            truck_state[truck_id] = {
                "temperature": temperature,
                "timestamp": timestamp
            }

            high_count = sum(
                1 for truck in truck_state.values()
                if truck["temperature"] >= 35
            )

            normal_count = len(truck_state) - high_count

            if temperature >= 35:
                status = "HIGH TEMPERATURE"
            else:
                status = "NORMAL"

            print(
                f"Truck: {truck_id} | "
                f"Temperature: {temperature}°C | "
                f"Status: {status}"
            )

            print(
                f"Fleet Summary → "
                f"Total: {len(truck_state)} | "
                f"Normal: {normal_count} | "
                f"High: {high_count}"
            )

            print("-" * 60)

        except (json.JSONDecodeError, KeyError) as e:
            print("Invalid telemetry:", e)

except KeyboardInterrupt:
    print("\nWorker stopped.")

finally:
    consumer.close()