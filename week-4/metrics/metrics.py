from prometheus_client import Counter, Gauge, Histogram, start_http_server
import time


# Total number of events processed
events_processed = Counter(
    "streamforge_events_processed_total",
    "Total number of telemetry events processed",
    ["worker"]
)


# Current events processed per second
events_per_second = Gauge(
    "streamforge_events_per_second",
    "Current event processing rate",
    ["worker"]
)


# Processing lag in milliseconds
processing_lag = Gauge(
    "streamforge_processing_lag_ms",
    "Telemetry processing lag in milliseconds",
    ["worker"]
)


# Worker status: 1 = running, 0 = stopped
worker_status = Gauge(
    "streamforge_worker_status",
    "Current worker status",
    ["worker"]
)


# Processing time for each event
processing_time = Histogram(
    "streamforge_processing_time_seconds",
    "Time taken to process each telemetry event",
    ["worker"]
)


# Start Prometheus metrics server
def start_metrics_server(port=8000):
    start_http_server(port)
    print(f"Prometheus metrics server started on port {port}")


# Simple events-per-second tracker
class EventRateTracker:

    def __init__(self):
        self.start_time = time.time()
        self.event_count = 0

    def record_event(self):
        self.event_count += 1

    def get_rate(self):
        elapsed = time.time() - self.start_time

        if elapsed <= 0:
            return 0

        return self.event_count / elapsed