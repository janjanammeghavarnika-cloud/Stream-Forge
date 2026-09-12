import json
from confluent_kafka import Consumer
from state_manager import StateManager


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-workers-v4",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["test-topic"])

state_manager = StateManager("week-3/state/worker-2")

print("StreamForge Worker 2 started...")
print("Worker 2 processing assigned partitions...")


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

            state_manager.save_reading(
                truck_id,
                temperature,
                timestamp
            )

            rolling_average = state_manager.get_rolling_average(
                truck_id
            )

            if temperature >= 35:
                status = "HIGH TEMPERATURE"
            else:
                status = "NORMAL"

            print(
                f"Truck: {truck_id} | "
                f"Temperature: {temperature}°C | "
                f"5-Min Average: {rolling_average:.2f}°C | "
                f"Status: {status}"
            )

            print("Worker 2 state saved to RocksDB ✓")
            print("-" * 70)

        except (json.JSONDecodeError, KeyError) as e:
            print("Invalid telemetry:", e)

except KeyboardInterrupt:
    print("\nWorker 2 stopped.")

finally:
    state_manager.close()
    consumer.close()
