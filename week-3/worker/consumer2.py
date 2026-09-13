import json
from confluent_kafka import Consumer
from state_manager import StateManager


# Main telemetry consumer
consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-workers-v4",
    "auto.offset.reset": "earliest",
})

# Separate consumer for the Kafka state changelog
changelog_consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-state-recovery-v2",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["test-topic"])
changelog_consumer.subscribe(["streamforge-state-changelog"])


state_manager = StateManager("week-3/state/worker-2")

print("StreamForge Worker 2 started...")
print("Worker 2 processing assigned partitions...")
print("Checking Kafka changelog for state recovery...")


# --------------------------------------------------
# Recover previous state from Kafka changelog
# --------------------------------------------------
recovered_count = 0
idle_polls = 0

while idle_polls < 5:

    msg = changelog_consumer.poll(1.0)

    if msg is None:
        idle_polls += 1
        continue

    idle_polls = 0

    if msg.error():
        print("Changelog Kafka error:", msg.error())
        continue

    try:
        changelog_data = json.loads(
            msg.value().decode("utf-8")
        )

        truck_id = changelog_data["truck_id"]
        recovered_state = changelog_data["state"]

        state_manager.restore_state(
            truck_id,
            recovered_state
        )

        recovered_count += 1

    except (json.JSONDecodeError, KeyError) as e:
        print("Invalid changelog record:", e)

    if msg.error():
        print("Changelog Kafka error:", msg.error())
        break

    try:
        changelog_data = json.loads(
            msg.value().decode("utf-8")
        )

        truck_id = changelog_data["truck_id"]
        recovered_state = changelog_data["state"]

        state_manager.restore_state(
            truck_id,
            recovered_state
        )

        recovered_count += 1

    except (json.JSONDecodeError, KeyError) as e:
        print("Invalid changelog record:", e)


print(
    f"State recovery completed. "
    f"Recovered {recovered_count} state records."
)

changelog_consumer.close()


# --------------------------------------------------
# Process live telemetry
# --------------------------------------------------

try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:

            data = json.loads(
                msg.value().decode("utf-8")
            )

            truck_id = data["truck_id"]
            temperature = data["temperature"]
            timestamp = data["timestamp"]

            # Save new telemetry reading
            state_manager.save_reading(
                truck_id,
                temperature,
                timestamp
            )

            # Calculate rolling average
            rolling_average = state_manager.get_rolling_average(
                truck_id
            )

            # Determine temperature status
            if temperature >= 35:
                status = "HIGH TEMPERATURE"
            else:
                status = "NORMAL"

            print(
                f"Truck: {truck_id} | "
                f"Temperature: {temperature}°C | "
                f"5-Min Average: "
                f"{rolling_average:.2f}°C | "
                f"Status: {status}"
            )

            print(
                "Worker 2 state saved to RocksDB ✓"
            )

            print("-" * 70)

        except (json.JSONDecodeError, KeyError) as e:

            print("Invalid telemetry:", e)


except KeyboardInterrupt:

    print("\nWorker 2 stopped.")


finally:

    state_manager.close()
    consumer.close()