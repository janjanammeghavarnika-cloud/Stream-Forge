from http.server import BaseHTTPRequestHandler, HTTPServer
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from pathlib import Path
import sys

# Allow importing Week 4 metrics
sys.path.append(str(Path(__file__).resolve().parent.parent))

from metrics.metrics import (
    events_processed,
    events_per_second,
    processing_lag,
    worker_status,
    processing_time,
    start_metrics_server,
    EventRateTracker
)


class MetricsHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path == "/metrics":

            metrics = generate_latest()

            self.send_response(200)
            self.send_header(
                "Content-Type",
                CONTENT_TYPE_LATEST
            )
            self.send_header(
                "Content-Length",
                str(len(metrics))
            )
            self.end_headers()

            self.wfile.write(metrics)

        else:

            self.send_response(404)
            self.end_headers()

            self.wfile.write(
                b"StreamForge Metrics API - use /metrics"
            )

    def log_message(self, format, *args):
        return


def start_backend(port=8000):

    server = HTTPServer(
        ("localhost", port),
        MetricsHandler
    )

    print("----------------------------------------")
    print("StreamForge Week 4 Metrics Backend")
    print("----------------------------------------")
    print(f"Metrics endpoint: http://localhost:{port}/metrics")
    print("Backend running...")
    print("Press CTRL+C to stop.")
    print("----------------------------------------")

    # Mark backend worker as running
    worker_status.labels(worker="backend").set(1)

    try:
        server.serve_forever()

    except KeyboardInterrupt:

        print("\nBackend stopped.")

    finally:

        worker_status.labels(worker="backend").set(0)
        server.server_close()


if __name__ == "__main__":
    start_backend()