import json
from confluent_kafka import Consumer, Producer
from state_manager import StateManager


consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-workers-v4",
    "auto.offset.reset": "earliest",
})

changelog_producer = Producer({
    "bootstrap.servers": "localhost:9092"
})

consumer.subscribe(["test-topic"])

state_manager = StateManager("week-3/state/worker-1")

print("StreamForge Worker 1 started...")
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

            # Save telemetry reading in RocksDB
            state_manager.save_reading(
                truck_id,
                temperature,
                timestamp
            )

            # Calculate 5-minute rolling average
            rolling_average = state_manager.get_rolling_average(
                truck_id
            )

            # Get complete current state for recovery
            truck_state = state_manager.get_state(truck_id)

            changelog_data = {
                "truck_id": truck_id,
                "state": truck_state,
                "rolling_average": rolling_average,
                "timestamp": timestamp
            }

            # Backup complete state to Kafka changelog
            changelog_producer.produce(
                "streamforge-state-changelog",
                key=truck_id,
                value=json.dumps(changelog_data)
            )

            changelog_producer.flush()

            # Determine current temperature status
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

            print("State saved to RocksDB ✓")
            print("Complete state backed up to Kafka changelog ✓")
            print("-" * 70)

        except (json.JSONDecodeError, KeyError) as e:
            print("Invalid telemetry:", e)

except KeyboardInterrupt:
    print("\nWorker stopped.")

finally:
    state_manager.close()
    consumer.close()
    changelog_producer.flush()