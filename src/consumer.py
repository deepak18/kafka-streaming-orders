import json

from confluent_kafka import Consumer

consumer_config = {
    # provides the initial hosts that act as a starting point for
    # Kafka client to discover the full set of alive servers in the cluster
    "bootstrap.servers": "localhost:9092",
    # a unique string that identifies the consumer group this consumer belongs to
    "group.id": "order-tracker",
    # which offset to use when there is no initial offset in Kafka
    "auto.offset.reset": "earliest"
}

consumer = Consumer(consumer_config)

# TODO: Multiple consumers in one group, messages getting balanced across
# Register interest in a topic
consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to orders topic")

try:
    while True:
        # Consumer "poll" for a new message, instead of Kafka "push" it
        # Allows consumer to control the frequency for different events they consume
        msg = consumer.poll(1.0)    # Asks the broker for any new messages
        if msg is None:
            continue
        if msg.error():
            print("❌ Error:", msg.error())
            continue

        value = msg.value().decode("utf-8")
        order = json.loads(value)
        print(f"📦 Received order: {order['quantity']} x {order['item']} from {order['user']}")
except KeyboardInterrupt:
    print("\n🔴 Stopping consumer")

finally:
    # properly releasing resources associated with a consumer instance
    # ensures file handles are closed, offsets are commited, partition assignments are revoked
    consumer.close()
