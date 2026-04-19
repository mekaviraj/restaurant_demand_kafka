# dashboard/app.py
"""
Food Delivery Order Reliability Dashboard
=========================================

Real-time visualization of food order reliability under Kafka
delivery guarantees: at-most-once, at-least-once, exactly-once.

Accessible at: http://localhost:5000
"""

from flask import Flask, jsonify, render_template
import threading
import json
from confluent_kafka import Consumer
from collections import defaultdict, deque
from datetime import datetime

app = Flask(__name__)

# ============================================
# Metrics Tracking Structure
# ============================================
metrics_lock = threading.Lock()

metrics = {
    "last_updated": None,
    "current_mode": "unknown",
    "modes": {
        "at_most_once": {
            "total_orders": 0,
            "unique_orders": 0,
            "duplicate_orders": 0,
            "duplicate_percentage": 0.0,
            "accuracy": 100.0,
            "estimated_loss": 0,
            "max_sent_sequence": 0,
        },
        "at_least_once": {
            "total_orders": 0,
            "unique_orders": 0,
            "duplicate_orders": 0,
            "duplicate_percentage": 0.0,
            "accuracy": 100.0,
            "estimated_loss": 0,
            "max_sent_sequence": 0,
        },
        "exactly_once": {
            "total_orders": 0,
            "unique_orders": 0,
            "duplicate_orders": 0,
            "duplicate_percentage": 0.0,
            "accuracy": 100.0,
            "estimated_loss": 0,
            "max_sent_sequence": 0,
        }
    },
    "order_ids_seen": defaultdict(set),
    "recent_orders": deque(maxlen=12),
}

# ============================================
# Kafka Listener Thread
# ============================================
def kafka_listener():
    """
    Consume order stream from metrics topic and update reliability stats.
    """
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "dashboard_consumer",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    }
    
    consumer = Consumer(conf)
    consumer.subscribe(["metrics"])
    
    print("🎬 Dashboard Kafka Listener started...")
    print("   Consuming from 'metrics' topic")
    print("   Press Ctrl+C in browser to stop\n")

    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                print(f"⚠️  Kafka error: {msg.error()}")
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                mode = data.get("kafka_mode", "unknown").lower()
                order_id = data.get("order_id")
                sent_sequence = int(data.get("sent_sequence", 0))
                
                if mode not in metrics["modes"]:
                    continue
                
                with metrics_lock:
                    mode_stats = metrics["modes"][mode]
                    mode_stats["total_orders"] += 1
                    mode_stats["max_sent_sequence"] = max(mode_stats["max_sent_sequence"], sent_sequence)

                    # Ensure uniqueness is calculated after possible insertion.
                    if order_id in metrics["order_ids_seen"][mode]:
                        mode_stats["duplicate_orders"] += 1
                    else:
                        metrics["order_ids_seen"][mode].add(order_id)

                    mode_stats["unique_orders"] = len(metrics["order_ids_seen"][mode])

                    total_orders = mode_stats["total_orders"]
                    duplicates = mode_stats["duplicate_orders"]
                    unique_orders = mode_stats["unique_orders"]

                    mode_stats["duplicate_percentage"] = (
                        (duplicates * 100.0 / total_orders) if total_orders > 0 else 0.0
                    )
                    mode_stats["accuracy"] = (
                        (unique_orders * 100.0 / total_orders) if total_orders > 0 else 100.0
                    )
                    mode_stats["estimated_loss"] = max(
                        mode_stats["max_sent_sequence"] - total_orders,
                        0,
                    )

                    metrics["recent_orders"].appendleft({
                        "order_id": order_id,
                        "restaurant_name": data.get("restaurant_name", "N/A"),
                        "item_name": data.get("item_name", "N/A"),
                        "total_price": data.get("total_price", 0),
                        "kafka_mode": mode,
                    })

                    metrics["current_mode"] = mode
                    metrics["last_updated"] = datetime.now().isoformat()
                    
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Listener error: {e}")

# Start listener in background thread
listener_thread = threading.Thread(target=kafka_listener, daemon=True)
listener_thread.start()

# ============================================
# Flask Routes
# ============================================

@app.route("/")
def index():
    """Serve the dashboard HTML"""
    return render_template("index.html")

@app.route("/api/metrics")
def get_metrics():
    """
    API endpoint for dashboard cards.
    Returns normalized payload for current mode and all modes.
    """
    with metrics_lock:
        active_mode = metrics["current_mode"]
        active_stats = metrics["modes"].get(active_mode, {
            "total_orders": 0,
            "unique_orders": 0,
            "duplicate_orders": 0,
            "duplicate_percentage": 0.0,
            "accuracy": 100.0,
            "estimated_loss": 0,
        })

        summary = {
            "timestamp": metrics["last_updated"],
            "kafka_mode": active_mode,
            "total_orders": active_stats["total_orders"],
            "unique_orders": active_stats["unique_orders"],
            "duplicate_orders": active_stats["duplicate_orders"],
            "duplicate_percentage": active_stats["duplicate_percentage"],
            "accuracy": active_stats["accuracy"],
            "estimated_loss": active_stats["estimated_loss"],
            "modes": metrics["modes"],
            "recent_orders": list(metrics["recent_orders"]),
        }

        return jsonify(summary)

@app.route("/api/semantics-explanation")
def semantics_explanation():
    """Detailed explanation of Kafka delivery semantics"""
    return jsonify({
        "semantics": {
            "at_most_once": {
                "name": "🚨 At-Most-Once",
                "description": "Fast delivery, but some food orders may be lost",
                "impact": "Customer may never receive placed order",
            },
            "at_least_once": {
                "name": "📋 At-Least-Once",
                "description": "No order loss, but duplicate order processing may happen",
                "impact": "Restaurant may prepare same order twice",
            },
            "exactly_once": {
                "name": "✅ Exactly-Once",
                "description": "No loss and no duplicate orders",
                "impact": "Correct for payments and inventory pipelines",
            }
        }
    })

# ============================================
# Error Handler
# ============================================
@app.errorhandler(404)
def not_found(error):
    return jsonify({"error": "Not found"}), 404

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🌐 FOOD DELIVERY ORDER RELIABILITY DASHBOARD")
    print("="*70)
    print("Starting Flask server on http://localhost:5000")
    print("\nOpen browser and visit: http://localhost:5000")
    print("="*70 + "\n")
    
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)