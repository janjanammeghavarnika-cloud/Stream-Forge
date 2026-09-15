import json
import time
import sys
from pathlib import Path

from confluent_kafka import Consumer, Producer
from state_manager import StateManager


# --------------------------------------------------
# Load Week 4 Prometheus metrics
# --------------------------------------------------

week4_metrics_path = (
    Path(__file__).resolve().parent.parent.parent
    / "week-4"
    / "metrics"
)

sys.path.append(str(week4_metrics_path))

from metrics import (
    events_processed,
    events_per_second,
    processing_lag,
    worker_status,
    processing_time,
    EventRateTracker,
    start_metrics_server
)

# --------------------------------------------------
# Kafka Consumer
# --------------------------------------------------

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-workers-v4",
    "auto.offset.reset": "earliest",
})


# --------------------------------------------------
# Kafka Changelog Producer
# --------------------------------------------------

changelog_producer = Producer({
    "bootstrap.servers": "localhost:9092"
})


consumer.subscribe(["test-topic"])


# --------------------------------------------------
# RocksDB State Manager
# --------------------------------------------------

state_manager = StateManager(
    "week-3/state/worker-1"
)


# --------------------------------------------------
# Week 4 Metrics
# --------------------------------------------------

worker_name = "worker-1"
rate_tracker = EventRateTracker()
start_metrics_server(8001)

worker_status.labels(
    worker=worker_name
).set(1)


print("StreamForge Worker 1 started...")
print("Live fleet monitoring active...")
print("Prometheus metrics active for Worker 1...")


# --------------------------------------------------
# Main Processing Loop
# --------------------------------------------------

try:

    while True:

        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():

            print("Kafka error:", msg.error())
            continue

        # Start processing timer
        processing_start = time.time()

        try:

            # --------------------------------------------------
            # Decode telemetry
            # --------------------------------------------------

            data = json.loads(
                msg.value().decode("utf-8")
            )

            truck_id = data["truck_id"]
            temperature = data["temperature"]
            timestamp = data["timestamp"]


            # --------------------------------------------------
            # Save telemetry reading in RocksDB
            # --------------------------------------------------

            state_manager.save_reading(
                truck_id,
                temperature,
                timestamp
            )


            # --------------------------------------------------
            # Calculate 5-minute rolling average
            # --------------------------------------------------

            rolling_average = (
                state_manager.get_rolling_average(
                    truck_id
                )
            )


            # --------------------------------------------------
            # Get complete current state
            # --------------------------------------------------

            truck_state = (
                state_manager.get_state(
                    truck_id
                )
            )


            # --------------------------------------------------
            # Create changelog record
            # --------------------------------------------------

            changelog_data = {

                "truck_id": truck_id,

                "state": truck_state,

                "rolling_average": rolling_average,

                "timestamp": timestamp
            }


            # --------------------------------------------------
            # Backup state to Kafka changelog
            # --------------------------------------------------

            changelog_producer.produce(

                "streamforge-state-changelog",

                key=truck_id,

                value=json.dumps(
                    changelog_data
                )
            )

            changelog_producer.flush()


            # --------------------------------------------------
            # Determine temperature status
            # --------------------------------------------------

            if temperature >= 35:

                status = "HIGH TEMPERATURE"

            else:

                status = "NORMAL"


            # --------------------------------------------------
            # Week 4 Metrics
            # --------------------------------------------------

            # Count processed event
            events_processed.labels(
                worker=worker_name
            ).inc()


            # Update events-per-second
            rate_tracker.record_event()

            current_rate = (
                rate_tracker.get_rate()
            )

            events_per_second.labels(
                worker=worker_name
            ).set(current_rate)

            try:

                from datetime import datetime

                event_datetime = (
                    datetime.fromisoformat(
                        timestamp
                    )
                )

                current_datetime = datetime.now(
                    event_datetime.tzinfo
                )

                lag_seconds = (
                    current_datetime -
                    event_datetime
                ).total_seconds()

                processing_lag.labels(
                    worker=worker_name
                ).set(
                    max(0, lag_seconds * 1000)
                )

            except Exception:

                pass


            # Record processing time
            processing_duration = (
                time.time() -
                processing_start
            )

            processing_time.labels(
                worker=worker_name
            ).observe(
                processing_duration
            )


            # --------------------------------------------------
            # Console Output
            # --------------------------------------------------

            print(
                f"Truck: {truck_id} | "
                f"Temperature: {temperature}°C | "
                f"5-Min Average: "
                f"{rolling_average:.2f}°C | "
                f"Status: {status}"
            )

            print(
                "State saved to RocksDB ✓"
            )

            print(
                "Complete state backed up "
                "to Kafka changelog ✓"
            )

            print(
                f"Events/sec: "
                f"{current_rate:.2f}"
            )

            print(
                f"Processing time: "
                f"{processing_duration * 1000:.2f} ms"
            )

            print("-" * 70)


        except (
            json.JSONDecodeError,
            KeyError
        ) as e:

            print(
                "Invalid telemetry:",
                e
            )


# --------------------------------------------------
# Stop Worker
# --------------------------------------------------

except KeyboardInterrupt:

    print("\nWorker stopped.")


finally:

    # Mark Worker 1 as stopped
    worker_status.labels(
        worker=worker_name
    ).set(0)

    state_manager.close()

    consumer.close()

    changelog_producer.flush()

    print(
        "Worker 1 metrics stopped."
    )