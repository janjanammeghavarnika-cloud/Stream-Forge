import json
import time
import sys
from pathlib import Path
from datetime import datetime

from confluent_kafka import Consumer
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
# Main telemetry consumer
# --------------------------------------------------

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-workers-v4",
    "auto.offset.reset": "earliest",
})


# --------------------------------------------------
# Separate consumer for Kafka state changelog
# --------------------------------------------------

changelog_consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-state-recovery-v2",
    "auto.offset.reset": "latest",
})


consumer.subscribe(["test-topic"])
changelog_consumer.subscribe(["streamforge-state-changelog"])


# --------------------------------------------------
# RocksDB State Manager
# --------------------------------------------------

state_manager = StateManager(
    "week-3/state/worker-2"
)


# --------------------------------------------------
# Week 4 Metrics
# --------------------------------------------------

worker_name = "worker-2"

rate_tracker = EventRateTracker()

start_metrics_server(8002)

worker_status.labels(
    worker=worker_name
).set(1)


print("StreamForge Worker 2 started...")
print("Worker 2 processing assigned partitions...")
print("Checking Kafka changelog for state recovery...")
print("Prometheus metrics active for Worker 2...")


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

        print(
            "Invalid changelog record:",
            e
        )


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

            print(
                "Kafka error:",
                msg.error()
            )

            continue

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
            # Save telemetry reading
            # --------------------------------------------------

            state_manager.save_reading(
                truck_id,
                temperature,
                timestamp
            )


            # --------------------------------------------------
            # Calculate rolling average
            # --------------------------------------------------

            rolling_average = (
                state_manager.get_rolling_average(
                    truck_id
                )
            )


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


            # Update events per second
            rate_tracker.record_event()

            current_rate = (
                rate_tracker.get_rate()
            )

            events_per_second.labels(
                worker=worker_name
            ).set(
                current_rate
            )


            # --------------------------------------------------
            # Calculate processing lag
            # --------------------------------------------------

            try:

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
                    max(
                        0,
                        lag_seconds * 1000
                    )
                )

            except Exception:

                pass


            # --------------------------------------------------
            # Record processing time
            # --------------------------------------------------

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
                "Worker 2 state saved to RocksDB ✓"
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


except KeyboardInterrupt:

    print("\nWorker 2 stopped.")


finally:

    # Mark Worker 2 as stopped
    worker_status.labels(
        worker=worker_name
    ).set(0)

    state_manager.close()

    consumer.close()

    print(
        "Worker 2 metrics stopped."
    )