# producer/producer.py
"""
Real-Time Food Delivery Order Producer
======================================

Demonstrates Kafka delivery guarantees with simulated order traffic:
1. at_most_once  -> possible loss (simulated drops)
2. at_least_once -> possible duplicates (simulated duplicates)
3. exactly_once  -> no loss, no duplicates

Usage:
    python producer.py [at_most_once | at_least_once | exactly_once]

Optional toggle:
    export KAFKA_MODE=at_least_once
    python producer.py
"""

from confluent_kafka import Producer
import json
import time
import random
import sys
import uuid
import os
from datetime import datetime

# Parse delivery mode
mode = sys.argv[1].lower() if len(sys.argv) > 1 else os.getenv("KAFKA_MODE", "exactly_once").lower()
if mode not in ["at_most_once", "at_least_once", "exactly_once"]:
    print(f"Invalid mode '{mode}'. Choose: at_most_once, at_least_once, exactly_once")
    sys.exit(1)

RESTAURANTS = ["Dominos", "KFC", "Biryani House", "Pizza Hut", "Local Dhaba"]
ITEMS = ["Burger", "Pizza", "Biryani", "Fried Rice", "Rolls"]
ITEM_PRICES = {
    "Burger": 129,
    "Pizza": 249,
    "Biryani": 219,
    "Fried Rice": 179,
    "Rolls": 149,
}

# ============================================
# Kafka Configuration per Delivery Semantic
# ============================================
kafka_config = {
    "bootstrap.servers": "localhost:9092",
    "client.id": f"producer-{mode}-{uuid.uuid4().hex[:8]}",
}

if mode == "at_most_once":
    # Fire-and-forget: Send and hope for the best
    # acks=0: Don't wait for any acknowledgment
    # retries=0: Don't retry on failure
    print("\n🚨 AT-MOST-ONCE MODE: Fast, but order loss is possible")
    kafka_config.update({
        "acks": "0",
        "retries": 0,
    })

elif mode == "at_least_once":
    # Guaranteed delivery but may duplicate: Persist before acknowledging
    # acks=all: Wait for all in-sync replicas to acknowledge
    # retries=10: Retry failed sends (can create duplicates if broker acks but fails to send to client)
    print("\n📋 AT-LEAST-ONCE MODE: No order loss, but duplicates are possible")
    kafka_config.update({
        "acks": "all",
        "retries": 10,
        "max.in.flight.requests.per.connection": 5,  # May cause reordering on retry
    })

elif mode == "exactly_once":
    # Exactly once: Idempotent + Transactional
    # enable.idempotence=True: Deduplicates retried sends
    # transactional.id: Enables transactions for atomic writes
    print("\n✅ EXACTLY-ONCE MODE: Clean processing, no loss and no duplicates")
    kafka_config.update({
        "acks": "all",
        "retries": 10,
        "enable.idempotence": True,
        "transactional.id": f"producer-{mode}-{uuid.uuid4().hex[:8]}",
        "max.in.flight.requests.per.connection": 5,
    })

producer = Producer(kafka_config)

# Initialize transactions for exactly-once mode
if mode == "exactly_once":
    producer.init_transactions()

# ============================================
# Delivery Report Callback
# ============================================
sent_count = 0
failed_count = 0

def delivery_report(err, msg):
    global sent_count, failed_count
    if err is not None:
        failed_count += 1
        print(f"  ❌ Delivery failed: {err}")
    else:
        sent_count += 1

# ============================================
# Incident Simulation Setup
# ============================================
def get_active_incident():
    state_file = "simulation_state.json"
    if os.path.exists(state_file):
        try:
            with open(state_file, "r") as f:
                data = json.load(f)
                return data.get("active_incident", "normal")
        except Exception:
            pass
    return "normal"

INCIDENT_CONFIGS = {
    "normal": {
        "delay_prob": 0.05,
        "prep_time_range": (12, 18),
        "cancel_prob": 0.02
    },
    "kitchen_delay": {
        "delay_prob": 0.45,
        "prep_time_range": (25, 38),
        "cancel_prob": 0.05
    },
    "courier_delay": {
        "delay_prob": 0.55,
        "prep_time_range": (12, 18),
        "cancel_prob": 0.15
    },
    "weather_surge": {
        "delay_prob": 0.65,
        "prep_time_range": (15, 22),
        "cancel_prob": 0.25
    },
    "staff_shortage": {
        "delay_prob": 0.35,
        "prep_time_range": (24, 32),
        "cancel_prob": 0.10
    },
    "festival_rush": {
        "delay_prob": 0.40,
        "prep_time_range": (28, 38),
        "cancel_prob": 0.08
    },
    "inventory_shortage": {
        "delay_prob": 0.15,
        "prep_time_range": (15, 20),
        "cancel_prob": 0.30
    }
}

# ============================================
# Order Generation
# ============================================
def gen_order(order_id=None, sent_sequence=0):
    """
    Generate a realistic food delivery order event.
    order_id is used downstream for deduplication.
    """
    if order_id is None:
        order_id = str(uuid.uuid4())

    item_name = random.choice(ITEMS)
    quantity = random.randint(1, 5)
    price_per_item = ITEM_PRICES[item_name]
    
    incident = get_active_incident()
    cfg = INCIDENT_CONFIGS.get(incident, INCIDENT_CONFIGS["normal"])
    
    prep_time = random.randint(*cfg["prep_time_range"])
    is_delayed = random.random() < cfg["delay_prob"]
    is_cancelled = random.random() < cfg["cancel_prob"]

    return {
        "order_id": order_id,
        "customer_id": f"cust_{random.randint(1000, 9999)}",
        "restaurant_name": random.choice(RESTAURANTS),
        "item_name": item_name,
        "quantity": quantity,
        "price_per_item": price_per_item,
        "total_price": quantity * price_per_item,
        "timestamp": datetime.utcnow().isoformat(),
        "kafka_mode": mode,
        "sent_sequence": sent_sequence,
        "producer_sent_at": time.time(),
        
        # Operational Simulation Metrics
        "prep_time_minutes": prep_time,
        "is_delayed": is_delayed,
        "is_cancelled": is_cancelled,
        "incident_mode": incident
    }

# ============================================
# Send Orders
# ============================================
print(f"\n{'='*60}")
print(f"Producer started in {mode.upper()} mode")
print(f"{'='*60}")

total_count = 0
dropped_simulated = 0
duplicate_injected = 0
try:
    while True:
        total_count += 1

        # Generate unique order
        order_id = f"ORD-{mode[:3].upper()}-{int(time.time()*1000)}-{random.randint(1000,9999)}"
        order = gen_order(order_id, sent_sequence=total_count)
        
        json_str = json.dumps(order)

        # Simulated behavior per mode (to make demo differences visible)
        if mode == "at_most_once" and random.random() < 0.20:
            dropped_simulated += 1
            if total_count % 5 == 0:
                incident = get_active_incident()
                print(f"\n[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Sent: {sent_count} | Failed: {failed_count} | Dropped(sim): {dropped_simulated} "
                      f"| Total Attempts: {total_count} (mode: {mode}) | Incident Mode: {incident.upper()}")
            time.sleep(random.uniform(0.5, 1.0))
            continue

        # Send based on mode
        if mode == "exactly_once":
            try:
                producer.begin_transaction()
                producer.produce(
                    "orders",
                    key=order_id.encode("utf-8"),
                    value=json_str.encode("utf-8"),
                    callback=delivery_report
                )
                producer.commit_transaction()
            except Exception as e:
                print(f"  ❌ Transaction failed: {e}")
                failed_count += 1
        else:
            producer.produce(
                "orders",
                key=order_id.encode("utf-8"),
                value=json_str.encode("utf-8"),
                callback=delivery_report
            )

            if mode == "at_least_once" and random.random() < 0.25:
                duplicate_injected += 1
                producer.produce(
                    "orders",
                    key=order_id.encode("utf-8"),
                    value=json_str.encode("utf-8"),
                    callback=delivery_report
                )

        # Flush to ensure sends are dispatched
        producer.flush(0)

        # Log progress
        if total_count % 5 == 0:
            incident = get_active_incident()
            print(f"\n[{datetime.now().strftime('%H:%M:%S')}] " +
                  f"Sent: {sent_count} | Failed: {failed_count} | Dropped(sim): {dropped_simulated} " +
                  f"| DupInjected(sim): {duplicate_injected} | Total Attempts: {total_count} " +
                  f"(mode: {mode}) | Incident Mode: {incident.upper()}")

        time.sleep(random.uniform(0.5, 1.0))

except KeyboardInterrupt:
    print("\n\n⏹  Producer interrupted. Flushing remaining messages...")
    producer.flush(30)
    print(f"\n📊 FINAL STATS:")
    print(f"   Total Attempted Orders: {total_count}")
    print(f"   Successfully Sent: {sent_count}")
    print(f"   Failed: {failed_count}")
    print(f"   Simulated Drops (at_most_once): {dropped_simulated}")
    print(f"   Simulated Duplicates (at_least_once): {duplicate_injected}")
    print(f"   Success Rate: {(sent_count/total_count*100):.1f}%" if total_count > 0 else "N/A")