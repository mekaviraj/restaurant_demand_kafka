# dashboard/app.py
from flask import Flask, jsonify, render_template
import threading
import queue
import json
from confluent_kafka import Consumer

app = Flask(__name__)

stats_queue = queue.Queue()
stats = {
    "total_orders": 0,
    "per_restaurant": {},
    "per_dish": {},
    "duplicates": 0,
    "mode_summary": {}
}

def kafka_listener():
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "dashboard",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    c = Consumer(conf)
    c.subscribe(["metrics"])

    while True:
        msg = c.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("Kafka error:", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode())
            stats_queue.put(data)
            c.commit(msg)  # simulate "after processing"
        except Exception as e:
            print(e)

listener_thread = threading.Thread(target=kafka_listener, daemon=True)
listener_thread.start()

@app.route("/")
def index():
    return render_template("index.html")

@app.route("/stats")
def get_stats():
    while not stats_queue.empty():
        item = stats_queue.get()
        mode = item.get("mode", "unknown")
        if mode not in stats["mode_summary"]:
            stats["mode_summary"][mode] = {"received": 0, "duplicates": 0}
        if item["type"] == "received":
            stats["total_orders"] += 1
            stats["mode_summary"][mode]["received"] += 1
        # you can also count duplicates in a real system via dedup logic

    return jsonify(stats)

if __name__ == "__main__":
    app.run(port=5000, debug=True)