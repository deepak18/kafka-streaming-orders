import json
import uuid

from confluent_kafka import Producer

producer_config = {
    # provides the initial hosts that act as a starting point for
    # Kafka client to discover the full set of alive servers in the cluster
    "bootstrap.servers": "localhost:9092"
}

producer = Producer(producer_config)

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivered {msg.value().decode("utf-8")}")
        print(f"✅ Delivered to {msg.topic()} : partition {msg.partition()} : at offset {msg.offset()}")

order = {
    "order_id": str(uuid.uuid4()),
    "user": "User Name",
    "item": "Item Name",
    "quantity": 1
}

value = json.dumps(order).encode("utf-8")

producer.produce(
    topic="orders",     # Also creates a new topic if it doesn't already exist
    value=value,
    callback=delivery_report
)

producer.flush()
