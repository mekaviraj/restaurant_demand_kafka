# producer/producer.py
from confluent_kafka import Producer
import json
import time
import random
import sys

mode = sys.argv[1] if len(sys.argv) > 1 else "at_most_once"

kafka_config = {
    "bootstrap.servers": "localhost:9092",
}

if mode == "at_most_once":
    kafka_config.update({"acks": "0", "retries": 0})
elif mode == "at_least_once":
    kafka_config.update({"acks": "all", "retries": 10})
elif mode == "exactly_once":
    kafka_config.update({
        "acks": "all",
        "retries": 10,
        "enable.idempotence": True,
        "transactional.id": "tx1",
    })

producer = Producer(kafka_config)

if mode == "exactly_once":
    producer.init_transactions()

def delivery_report(err, msg):
    pass  # ignore for now

def gen_order():
    return {
        "event_id": f"e{random.randint(1000,9999)}",
        "restaurant_id": f"r{random.randint(1,5)}",
        "dish_id": f"d{random.randint(1,10)}",
        "quantity": random.randint(1, 3),
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "state": "created"
    }

count = 0
while True:
    ev = gen_order()
    s = json.dumps(ev)

    if mode == "exactly_once":
        producer.begin_transaction()
        producer.produce("orders", s.encode("utf-8"), callback=delivery_report)
        producer.commit_transaction()
    else:
        producer.produce("orders", s.encode("utf-8"), callback=delivery_report)

    count += 1
    if count % 10 == 0:
        print(f"Sent {count} orders (mode: {mode}) ...")

    producer.flush()
    time.sleep(0.5)