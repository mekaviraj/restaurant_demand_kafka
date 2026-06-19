# Project Walkthrough: Agentic AI Operations Copilot

We have successfully transformed the real-time analytics platform into an **Agentic AI Operations Copilot** while preserving the existing Kafka event streaming and metrics pipeline.

Below is a summary of the architectural updates, component descriptions, and instructions on how to run and verify the transformed system.

---

## 1. Summary of Changes Made

We have reorganized the project structure to introduce a clean, modular layout with separate directories for Agents, Tools, RAG, Memory, and the LLM client:

### Core Code Additions & Enhancements:
1. **Producer Modification** ([producer.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/producer/producer.py)):
   * Integrated a shared state file reader (`simulation_state.json`) to dynamically switch between normal and anomaly simulation modes.
   * Enhanced the generated order payload with new fields: `prep_time_minutes`, `is_delayed`, `is_cancelled`, and `incident_mode`.
   * Added logs showing the current active incident simulation.
2. **PySpark Consumer Enhancement** ([consumer_spark.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/consumer/consumer_spark.py)):
   * Updated the schema and projection output to parse and forward the new operational simulation fields.
3. **Agent Implementation**:
   * **Incident Analyst Agent** ([analyst.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/agents/analyst.py)): Fetches metrics telemetry, analyzes anomalies, identifies likely root causes, and produces a structured incident report.
   * **Strategy Planner Agent** ([planner.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/agents/planner.py)): Consumes incident reports, retrieves matching operational SOPs via RAG and past similar incidents from Memory, formulates corrective directives, and projects expected improvement.
4. **Tools Development**:
   * **Metrics Tool** ([metrics_tool.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/tools/metrics_tool.py)): Decouples metrics fetching, supporting direct operational telemetry injection.
   * **RAG Tool** ([rag_tool.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/tools/rag_tool.py)): Integrates with the custom Vector database.
   * **Memory Tool** ([memory_tool.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/tools/memory_tool.py)): Integrates with SQLite to lookup and record incident histories.
5. **RAG & Memory Systems**:
   * **Vector Search Engine** ([vector_db.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/rag/vector_db.py)): Built a fast, zero-dependency, pure-Python TF-IDF and Cosine Similarity vector database to index operational SOPs.
   * **SQLite Store** ([database.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/memory/database.py)): Set up a persistent SQLite operational database (`memory/memory.db`) containing seeded baseline incident records.
   * **SOP Documents**: Added 6 standard operating procedures in `rag/sops/` for Kitchen Delays, Courier Delays, Staff Shortages, Festival Rushes, Inventory Shortages, and Weather Surges.
6. **LLM Client** ([client.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/llm/client.py)):
   * Designed a Gemini API wrapper that automatically falls back to a highly robust mock operational solver if no `GEMINI_API_KEY` is present.
7. **Dashboard Redesign**:
   * **Backend** ([app.py](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/dashboard/app.py)): Calculates operational statistics, handles automatic dual-topic routing (supports both direct Kafka order consumption and Spark streaming metrics forwarding), and exposes control and diagnostic API endpoints.
   * **Frontend** ([index.html](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/dashboard/templates/index.html)): Upgraded to a beautiful glassmorphic dark-theme design. Added tab navigation separating "AI Operations Copilot" and "Kafka Semantics", a control grid for injecting incidents, real-time telemetry card bindings, and scrolling agent trace logs.

---

## 2. Interactive Simulation Demonstration

The dashboard includes a **Simulation Command Center** which writes the active simulation state to `simulation_state.json`. The producer reads this state on each loop and adjusts its order generation parameters:

| Simulation Mode | Delay Probability | Prep Time Range | Cancellation Probability | Primary Diagnosed Cause |
|---|---|---|---|---|
| **Normal Operations** | 5% | 12 - 18 minutes | 2% | None / Healthy |
| **Kitchen Bottleneck** | 45% | 25 - 38 minutes | 5% | Kitchen Bottleneck |
| **Courier Shortage** | 55% | 12 - 18 minutes | 15% | Courier Shortage |
| **Weather Surge** | 65% | 15 - 22 minutes | 25% | Weather Surge |
| **Kitchen Staff Deficit** | 35% | 24 - 32 minutes | 10% | Staff Shortage |
| **Festival Rush** | 40% | 28 - 38 minutes | 8% | Festival Rush |
| **Inventory Shortage** | 15% | 15 - 20 minutes | 30% | Inventory Shortage |

### Automatic Threshold Breach Trigger:
When any of the metrics violate SLA targets (e.g., Delay Rate > 20%, cancellation rate > 10%, average prep time > 22 min) after at least 10 orders have been generated in an active incident simulation:
1. The Flask server automatically executes `run_ai_diagnostics()` in a background thread.
2. The Incident Analyst Agent evaluates the operational statistics.
3. The Strategy Planner Agent retrieves the matching SOP (via RAG) and past incident outcomes (via SQLite Memory) and generates mitigation steps.
4. The dashboard updates live with the corrective directives, RAG context, memory matches, and raw agent trace logs.

---

## 3. How to Run and Verify the System

### **Step 1: Install Dependencies**
Verify that all packages are installed:
```bash
pip install confluent-kafka flask pandas statsmodels google-generativeai
```

### **Step 2: Start Zookeeper & Kafka Broker**
Start your local Kafka environment as described in the [README.md](file:///c:/Users/VIRAJ%20M/Desktop/code/KAFKA_new/restaurant_demand_kafka/README.md).

### **Step 3: Start the Flask Dashboard**
In a dedicated terminal:
```bash
python dashboard/app.py
```
Open a browser and navigate to `http://localhost:5000`. You will see the new AI Operations Dashboard tab in active standby mode.

### **Step 4: Start the Kafka Producer**
In another terminal, start the producer in your chosen semantics mode (e.g., `exactly_once`):
```bash
python producer/producer.py exactly_once
```
Initially, it will run in **Normal Operations** mode. You will see orders arriving on the dashboard with normal metrics (Delay Rate ~5%, Prep Time ~15 min, Reliability Score ~98%).

### **Step 5: Inject an Incident**
1. On the dashboard, navigate to the **AI Operations Copilot** tab.
2. Click **🍳 Kitchen Bottleneck** in the Simulation Command Center.
3. The producer's terminal will display `Incident Mode: KITCHEN_DELAY` and begin producing delayed, high-prep-time orders.
4. On the dashboard, watch the **Average Prep Time** climb past 22 minutes and **Delay Rate** spike.
5. As soon as the threshold is breached, the status indicator will turn red, and the AI Operations Copilot will trigger.
6. The cards for **Diagnosed Issue**, **Confidence**, **Expected SLA Recovery**, and **Corrective Directives** will update with the plan.
7. Scroll down to inspect the raw thought processes in the **Analyst** and **Planner** agent trace consoles!

### **Step 6: Return to Normal**
1. Click **✅ Normal Operations** in the simulation command center.
2. The metrics will reset, the AI status indicator will return to green, and the copilot will go back to standby mode.
