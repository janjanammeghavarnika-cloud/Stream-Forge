from http.server import BaseHTTPRequestHandler, HTTPServer
from urllib.request import urlopen
from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram
import re
import time


WORKER_URLS = {
    "worker-1": "http://localhost:8001/metrics",
    "worker-2": "http://localhost:8002/metrics"
}


def fetch_worker_metrics():
    all_metrics = []

    for worker, url in WORKER_URLS.items():

        try:
            with urlopen(url, timeout=2) as response:
                data = response.read().decode("utf-8")

            all_metrics.append(
                f"# Worker: {worker}\n{data}"
            )

        except Exception as e:

            all_metrics.append(
                f"# Worker: {worker} unavailable\n"
                f"# Error: {e}\n"
            )

    return "\n".join(all_metrics).encode("utf-8")


class MetricsHandler(BaseHTTPRequestHandler):

    def do_GET(self):

        if self.path == "/metrics":

            metrics = fetch_worker_metrics()

           
            self.send_response(200)

            self.send_header(
    "Access-Control-Allow-Origin",
    "*"
)

            self.send_header(
            "Content-Type",
            "text/plain; version=0.0.4"
            )

            self.send_header(
                "Content-Length",
                str(len(metrics))
            )

            self.end_headers()

            self.wfile.write(metrics)

        elif self.path == "/":

            message = (
                b"StreamForge Week 4 Metrics Backend\n"
                b"Use /metrics to view worker metrics."
            )

            self.send_response(200)
            self.send_header(
                "Content-Type",
                "text/plain"
            )
            self.send_header(
                "Content-Length",
                str(len(message))
            )
            self.end_headers()

            self.wfile.write(message)

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
    print("Worker 1 source: http://localhost:8001/metrics")
    print("Worker 2 source: http://localhost:8002/metrics")
    print("Backend running...")
    print("Press CTRL+C to stop.")
    print("----------------------------------------")

    try:
        server.serve_forever()

    except KeyboardInterrupt:

        print("\nBackend stopped.")

    finally:

        server.server_close()


if __name__ == "__main__":
    start_backend()