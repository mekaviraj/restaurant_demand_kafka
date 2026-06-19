# dashboard/app.py
"""
Food Delivery Order Reliability & AI Operations Copilot Dashboard
==================================================================

Real-time visualization of food order reliability under Kafka
delivery guarantees, combined with an Agentic AI Operations Copilot.

Accessible at: http://localhost:5000
"""

from flask import Flask, jsonify, render_template, request
import threading
import json
import os
from confluent_kafka import Consumer
from collections import defaultdict, deque
from datetime import datetime

# Initialize Flask
app = Flask(__name__)

# Shared state file for producer simulation
STATE_FILE = "simulation_state.json"

# Initialize simulation state file to normal if not exists
if not os.path.exists(STATE_FILE):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump({"active_incident": "normal"}, f)
    except Exception as e:
        print(f"⚠️ Could not initialize simulation state file: {e}")

# ============================================
# Metrics Tracking Structure
# ============================================
metrics_lock = threading.Lock()
last_run_incident = None  # Track the last incident we ran AI diagnostics for to prevent spamming

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
    
    # Operational metrics
    "operational": {
        "total_orders": 0,
        "delayed_orders": 0,
        "delay_rate": 0.0,
        "total_prep_time": 0.0,
        "avg_prep_time": 0.0,
        "cancelled_orders": 0,
        "cancellation_rate": 0.0,
        "reliability_score": 100.0,
        "active_incident": "normal"
    },
    
    # AI Operations Copilot Output
    "ai_copilot": {
        "last_triggered": None,
        "current_incident": "None",
        "root_cause": "N/A",
        "confidence": "N/A",
        "evidence": [],
        "actions": [],
        "retrieved_sop": "N/A",
        "sop_content": "No SOP retrieved yet.",
        "improvement": "N/A",
        "past_incidents": [],
        "analyst_trace": "Analyst Agent is standby. Waiting for metrics threshold breach...",
        "planner_trace": "Strategy Planner Agent is standby. Waiting for analyst report..."
    }
}

# ============================================
# AI Copilot Execution Logic
# ============================================
def run_ai_diagnostics():
    """
    Execute the Incident Analyst and Strategy Planner agents in the background.
    """
    global last_run_incident
    
    # Add a delay so metrics have time to populate under the new simulation mode
    import time
    time.sleep(2.0)
    
    with metrics_lock:
        current_metrics = dict(metrics["operational"])
        incident_mode = current_metrics["active_incident"]
    
    # Don't run diagnostics if the incident is normal
    if incident_mode == "normal":
        return
        
    print(f"🤖 AI Operations Copilot: Threshold breach detected! Running diagnostics for incident: {incident_mode.upper()}...")
    
    # Set traces to running state
    with metrics_lock:
        metrics["ai_copilot"]["analyst_trace"] = "Analyst Agent is actively analyzing metrics..."
        metrics["ai_copilot"]["planner_trace"] = "Strategy Planner Agent is waiting for Analyst output..."
        
    try:
        from agents.analyst import IncidentAnalystAgent
        from agents.planner import StrategyPlannerAgent
        
        # 1. Run Incident Analyst Agent
        analyst = IncidentAnalystAgent(metrics_source=current_metrics)
        parsed_report, analyst_log = analyst.analyze()
        
        # 2. Run Strategy Planner Agent
        planner = StrategyPlannerAgent()
        parsed_plan, planner_log = planner.formulate_plan(parsed_report)
        
        # 3. Write results back to global metrics
        with metrics_lock:
            copilot = metrics["ai_copilot"]
            copilot["last_triggered"] = datetime.now().isoformat()
            copilot["current_incident"] = parsed_report.get("issue", "None")
            copilot["root_cause"] = parsed_report.get("issue", "N/A")
            copilot["confidence"] = parsed_report.get("confidence", "N/A")
            copilot["evidence"] = parsed_report.get("evidence", [])
            
            copilot["actions"] = parsed_plan.get("actions", [])
            copilot["retrieved_sop"] = parsed_plan.get("sop", "N/A")
            copilot["sop_content"] = parsed_plan.get("sop_content", "No SOP content.")
            copilot["improvement"] = parsed_plan.get("improvement", "N/A")
            copilot["past_incidents"] = parsed_plan.get("past_incidents", [])
            
            copilot["analyst_trace"] = analyst_log
            copilot["planner_trace"] = planner_log
            
        print("🤖 AI Operations Copilot: Diagnostics completed successfully!")
    except Exception as e:
        print(f"❌ AI Operations Copilot Diagnostics Failed: {e}")
        with metrics_lock:
            metrics["ai_copilot"]["analyst_trace"] = f"Error running Analyst: {e}"
            metrics["ai_copilot"]["planner_trace"] = "Analysis aborted due to analyst error."

# ============================================
# Kafka Listener Thread
# ============================================
def kafka_listener():
    """
    Consume order stream from Kafka and update reliability and operational metrics.
    Subscribes to BOTH 'metrics' and 'orders' topics to support Spark-free deployments.
    """
    global last_run_incident
    
    conf = {
        "bootstrap.servers": "localhost:9092",
        "group.id": "dashboard_consumer_group",
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
        "session.timeout.ms": 30000,
    }
    
    consumer = Consumer(conf)
    # Subscribe to orders (raw stream) and metrics (spark output)
    consumer.subscribe(["metrics", "orders"])
    
    print("🎬 Dashboard Kafka Listener started...")
    print("   Consuming from 'metrics' (Spark) and 'orders' (raw) topics")
    
    spark_active = False

    while True:
        try:
            msg = consumer.poll(timeout=1.0)
            
            if msg is None:
                continue
                
            if msg.error():
                print(f"⚠️ Kafka error: {msg.error()}")
                continue

            topic = msg.topic()
            
            # auto-detect if spark is writing to metrics
            if topic == "metrics":
                if not spark_active:
                    print("⚡ Spark consumer output detected on 'metrics' topic. Activating Spark forwarding mode.")
                    spark_active = True
            
            # If Spark is writing to metrics, ignore raw messages on 'orders' to avoid double counting
            if topic == "orders" and spark_active:
                continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                mode = data.get("kafka_mode", "unknown").lower()
                order_id = data.get("order_id")
                sent_sequence = int(data.get("sent_sequence", 0))
                
                if mode not in metrics["modes"]:
                    continue
                
                # Fetch operational details
                prep_time = int(data.get("prep_time_minutes", 15))
                is_delayed = bool(data.get("is_delayed", False))
                is_cancelled = bool(data.get("is_cancelled", False))
                incident_mode = data.get("incident_mode", "normal")
                
                with metrics_lock:
                    # 1. Update Kafka Semantics Stats
                    mode_stats = metrics["modes"][mode]
                    mode_stats["total_orders"] += 1
                    mode_stats["max_sent_sequence"] = max(mode_stats["max_sent_sequence"], sent_sequence)

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

                    # 2. Update Operational metrics
                    ops = metrics["operational"]
                    ops["total_orders"] += 1
                    ops["active_incident"] = incident_mode
                    
                    if is_delayed:
                        ops["delayed_orders"] += 1
                    if is_cancelled:
                        ops["cancelled_orders"] += 1
                    if not is_cancelled:
                        ops["total_prep_time"] += prep_time
                        
                    total_ops = ops["total_orders"]
                    ops["delay_rate"] = (ops["delayed_orders"] * 100.0 / total_ops) if total_ops > 0 else 0.0
                    ops["cancellation_rate"] = (ops["cancelled_orders"] * 100.0 / total_ops) if total_ops > 0 else 0.0
                    
                    non_cancelled_count = total_ops - ops["cancelled_orders"]
                    ops["avg_prep_time"] = (ops["total_prep_time"] / non_cancelled_count) if non_cancelled_count > 0 else 0.0
                    
                    # SLA Reliability Score Formula
                    ops["reliability_score"] = max(100.0 - (ops["delay_rate"] * 0.4) - (ops["cancellation_rate"] * 2.0), 0.0)

                    # Append to recent list
                    metrics["recent_orders"].appendleft({
                        "order_id": order_id,
                        "restaurant_name": data.get("restaurant_name", "N/A"),
                        "item_name": data.get("item_name", "N/A"),
                        "total_price": data.get("total_price", 0),
                        "kafka_mode": mode,
                        "status": "Cancelled" if is_cancelled else ("Delayed" if is_delayed else "Delivered"),
                        "prep_time": prep_time
                    })

                    metrics["current_mode"] = mode
                    metrics["last_updated"] = datetime.now().isoformat()
                    
                    # 3. Anomaly Detection Threshold Trigger
                    # Trigger agents once per incident if delay rate > 20% or cancellation rate > 10%
                    if total_ops >= 10 and incident_mode != "normal" and last_run_incident != incident_mode:
                        if ops["delay_rate"] > 20.0 or ops["cancellation_rate"] > 10.0 or ops["avg_prep_time"] > 22.0:
                            last_run_incident = incident_mode
                            threading.Thread(target=run_ai_diagnostics, daemon=True).start()
                    
            except Exception as e:
                print(f"❌ Error processing message: {e}")
                
        except KeyboardInterrupt:
            break
        except Exception as e:
            print(f"❌ Listener error: {e}")

# Start listener thread
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
    API endpoint for dashboard.
    Returns semantic metrics, operational metrics, and AI Copilot status.
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
            
            # Include new structures
            "operational": metrics["operational"],
            "ai_copilot": metrics["ai_copilot"]
        }

        return jsonify(summary)

@app.route("/api/simulate_incident", methods=["POST"])
def simulate_incident():
    """
    Endpoint to inject operational incident simulations into the system.
    """
    global last_run_incident
    
    data = request.json or {}
    incident = data.get("incident", "normal").lower()
    
    valid_incidents = ["normal", "kitchen_delay", "courier_delay", "weather_surge", "staff_shortage", "festival_rush", "inventory_shortage"]
    if incident not in valid_incidents:
        return jsonify({"error": f"Invalid incident mode. Must be one of {valid_incidents}"}), 400
        
    try:
        # Write mode to shared file
        with open(STATE_FILE, "w") as f:
            json.dump({"active_incident": incident}, f)
            
        with metrics_lock:
            # Update local tracking
            metrics["operational"]["active_incident"] = incident
            
            # Reset metrics stats to show clean progression under new simulation mode
            metrics["operational"]["total_orders"] = 0
            metrics["operational"]["delayed_orders"] = 0
            metrics["operational"]["delay_rate"] = 0.0
            metrics["operational"]["total_prep_time"] = 0.0
            metrics["operational"]["avg_prep_time"] = 0.0
            metrics["operational"]["cancelled_orders"] = 0
            metrics["operational"]["cancellation_rate"] = 0.0
            metrics["operational"]["reliability_score"] = 100.0
            metrics["recent_orders"].clear()
            
            # Reset AI agent trace logs and fields if returning to normal
            if incident == "normal":
                last_run_incident = None
                metrics["ai_copilot"] = {
                    "last_triggered": None,
                    "current_incident": "None",
                    "root_cause": "N/A",
                    "confidence": "N/A",
                    "evidence": [],
                    "actions": [],
                    "retrieved_sop": "N/A",
                    "sop_content": "No SOP retrieved yet.",
                    "improvement": "N/A",
                    "past_incidents": [],
                    "analyst_trace": "Analyst Agent is standby. Waiting for metrics threshold breach...",
                    "planner_trace": "Strategy Planner Agent is standby. Waiting for analyst report..."
                }
            else:
                # Force reset trigger tracking for the new incident
                last_run_incident = None
                metrics["ai_copilot"]["analyst_trace"] = f"Active incident '{incident.upper()}' injected. Simulating metric deterioration..."
                metrics["ai_copilot"]["planner_trace"] = "Strategy Planner Agent standby. Waiting for metrics threshold breach..."
                
        print(f"🚨 Simulation Mode Changed to: {incident.upper()}")
        return jsonify({"success": True, "active_incident": incident})
    except Exception as e:
        return jsonify({"error": f"Failed to modify simulation state: {e}"}), 500

@app.route("/api/ai_diagnose", methods=["POST"])
def force_ai_diagnose():
    """
    Endpoint to manually trigger AI diagnostics.
    """
    # Run in background to prevent request timeout
    threading.Thread(target=run_ai_diagnostics, daemon=True).start()
    return jsonify({"success": True, "message": "AI diagnostics triggered successfully in background."})

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
    print("🌐 FOOD DELIVERY ORDER RELIABILITY & AI OPERATIONS DASHBOARD")
    print("="*70)
    print("Starting Flask server on http://localhost:5000")
    print("\nOpen browser and visit: http://localhost:5000")
    print("="*70 + "\n")
    
    app.run(host="0.0.0.0", port=5000, debug=False, use_reloader=False)