from confluent_kafka import Consumer

consumer = Consumer({
    "bootstrap.servers": "localhost:9092",
    "group.id": "streamforge-workers",
    "auto.offset.reset": "earliest",
})

consumer.subscribe(["test-topic"])

print("StreamForge Worker started...")
print("Waiting for Kafka messages...")

try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue

        if msg.error():
            print("Kafka error:", msg.error())
            continue

        print(
            f"Received: {msg.value().decode('utf-8')} "
            f"| Partition: {msg.partition()} "
            f"| Offset: {msg.offset()}"
        )

except KeyboardInterrupt:
    print("\nWorker stopped.")

finally:
    consumer.close()