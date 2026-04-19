# Real-Time Food Delivery Order Reliability System using Kafka & Spark

This project simulates a Swiggy/Zomato-style backend where food orders stream in real time and are processed under different Kafka delivery guarantees.

## Why this project matters

- Lost orders mean a customer never receives food.
- Duplicate orders mean the restaurant may cook twice.
- Exactly-once is critical for payment and inventory integrity.

## What this project demonstrates

1. at_most_once
  - Fast delivery
  - Possible data loss (simulated drops)

2. at_least_once
  - No intentional loss
  - Possible duplicates (simulated duplicate sends)

3. exactly_once
  - No loss
  - No duplicates

The same architecture is used for all three modes, so behavior differences are easy to observe in the dashboard.

---

# 🔄 Kafka Delivery Semantics Explained

Before running the demo, understand the three **delivery semantics** that this project demonstrates:

## **1. 🚨 At-Most-Once (Fire-and-Forget)**
- **Producer Config:** `acks=0, retries=0`
- **Behavior:** Producer sends message and immediately returns (doesn't wait for acknowledgment)
- **Guarantee:** Message delivered 0 or 1 times
- **Risk:** ❌ **MESSAGE LOSS** - If broker fails before persisting, message is lost forever
- **Latency:** ⚡ Fastest (no waiting)
- **Use Case:** Metrics, application logs where losing a few events is acceptable
- **Expected in Dashboard:** 
  - `Received < Expected` (some messages lost)
  - No duplicates

## **2. 📋 At-Least-Once (Persistent)**
- **Producer Config:** `acks=all, retries=10, max.in.flight.requests=5`
- **Behavior:** Producer waits for **all in-sync replicas** to acknowledge. Retries on failure.
- **Guarantee:** Message delivered 1 or more times
- **Risk:** ⚠️ **DUPLICATES** - If producer retries after broker failure (but it actually succeeded), duplicate messages appear
- **Latency:** Medium (waits for replicas + retries)
- **Use Case:** Order processing (with deduplication logic), financial transactions
- **Expected in Dashboard:**
  - `Received > Unique Events` (duplicates detected)
  - No message loss

## **3. ✅ Exactly-Once (Guaranteed Exactly Once)**
- **Producer Config:** `acks=all, enable.idempotence=true, transactional.id=unique_id`
- **Behavior:** Idempotent producer + transactional writes. Automatically deduplicates retries.
- **Guarantee:** Message delivered **EXACTLY ONCE** per partition, per transaction
- **How:** Idempotence tracks producer sequence numbers; transactions ensure atomic multi-partition writes
- **Latency:** 🐢 Slowest (transactions + coordination)
- **Use Case:** Critical business data (payments, legal records, inventory)
- **Expected in Dashboard:**
  - `Received == Unique Events` (perfect 1:1 delivery)
  - Zero duplicates, zero loss

---

## 🧪 The Demo: What You'll See

When you run this project, you'll send restaurant orders through Kafka in three different modes (one at a time):

1. **Run Producer in AT-MOST-ONCE mode** → Some messages get lost
2. **Run Producer in AT-LEAST-ONCE mode** → See duplicates appear
3. **Run Producer in EXACTLY-ONCE mode** → Perfect delivery, no loss, no duplication

The **Dashboard** will show real-time metrics for each mode, making the delivery semantics **visually obvious**.

---

## 🚀 How to run (step‑by‑step)

### **Prerequisites**
- Linux/Mac with bash (or WSL2 on Windows)
- Java 11+
- Python 3.8+
- ~2GB disk space for Kafka

---

### **Step 0: Install Dependencies**

Open a terminal and run:

```bash
# Update package manager
sudo apt update
sudo apt upgrade -y

# Install Java 11 (required for Kafka/Zookeeper)
sudo apt install -y openjdk-11-jdk

# Verify Java installation
java -version

# Install Python dependencies
pip install pyspark confluent-kafka flask pandas statsmodels
```

---

### **Step 1: Download & Extract Kafka**

```bash
# Create workspace directory
mkdir -p ~/kafka_demo
cd ~/kafka_demo

# Download Kafka 3.4.0
wget https://archive.apache.org/dist/kafka/3.4.0/kafka_2.13-3.4.0.tgz

# Extract
tar -xzf kafka_2.13-3.4.0.tgz
cd kafka_2.13-3.4.0

# Verify structure
ls -la  # Should see: bin/, config/, libs/, etc.
```

---

### **Step 2: Start Zookeeper (Terminal 1)**

Zookeeper is required for Kafka cluster coordination. **Open a NEW terminal:**

```bash
cd ~/kafka_demo/kafka_2.13-3.4.0

# Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Expected output:**
```
INFO Listening on /0.0.0.0:2181 (org.apache.zookeeper.server.ZooKeeperServer)
```

✅ Leave this running, **don't close this terminal**.

---

### **Step 3: Start Kafka Broker (Terminal 2)**

**Open a SECOND terminal:**

```bash
cd ~/kafka_demo/kafka_2.13-3.4.0

# Start Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

**Expected output:**
```
INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
```

✅ Leave this running, **don't close this terminal**.

---

### **Step 4: Create Kafka Topics (Terminal 3)**

**Open a THIRD terminal:**

```bash
cd ~/kafka_demo/kafka_2.13-3.4.0

# Create topics
bin/kafka-topics.sh --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

bin/kafka-topics.sh --create \
  --topic metrics \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify topics were created
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Expected output:**
```
metrics
orders
```

✅ You can close this terminal now (topics are created).

---

### **Step 5: Start Spark Consumer (Terminal 4)**

This consumer watches the `orders` topic and detects duplicates.

**Open a FOURTH terminal:**

```bash
cd ~/restaurant_demand_kafka

# Start Spark Structured Streaming consumer
spark-submit consumer/consumer_spark.py
```

**Expected output:**
```
================================================
KAFKA DELIVERY SEMANTICS CONSUMER
================================================
[1/4] Reading from Kafka 'orders' topic...
[2/4] Setting up duplicate detection via event_id...
[3/4] Starting real-time metrics streams...
[4/4] Forwarding metrics to 'metrics' topic for dashboard...
```

✅ Leave this running, **don't close this terminal**. This watches for messages and detects duplicates.

---

### **Step 6: Start Flask Dashboard (Terminal 5)**

This is the **visual interface** to see delivery semantics in action.

**Open a FIFTH terminal:**

```bash
cd ~/restaurant_demand_kafka

# Start Flask dashboard
python dashboard/app.py
```

**Expected output:**
```
======================================================================
🌐 KAFKA DELIVERY SEMANTICS DASHBOARD
======================================================================
Starting Flask server on http://localhost:5000

Open browser and visit: http://localhost:5000
======================================================================
```

✅ Leave this running. Now **open browser → http://localhost:5000** and you'll see the dashboard.

---

### **Step 7: Run Producer (Test Each Semantic Mode) (Terminal 6)**

This is where the **magic happens**! You'll run the producer in three different modes to demonstrate delivery semantics.

**Open a SIXTH terminal:**

```bash
cd ~/restaurant_demand_kafka
```

#### **Mode 1: AT-MOST-ONCE (Fire-and-Forget)**
```bash
python producer/producer.py at_most_once
```

**Watch the output:**
```
============================================================
Producer started in AT-MOST-ONCE mode
============================================================

[HH:MM:SS] Sent: 45 | Failed: 0 | Total: 50 (mode: at_most_once)
```

**Watch the Dashboard:**
- You'll see messages come in, but some will be **lost** (fewer received than sent)
- No duplicates (fire-and-forget never retries)
- This demonstrates message loss with `acks=0`

**Run for ~30 seconds, then press Ctrl+C:**
```
⏹  Producer interrupted.
📊 FINAL STATS:
   Total Attempted: XXX
   Successfully Sent: YYY
   Failed: 0
```

---

#### **Mode 2: AT-LEAST-ONCE (Persistent with Retries)**

Wait ~5 seconds for dashboard to clear, then:

```bash
python producer/producer.py at_least_once
```

**Watch the output:**
```
============================================================
Producer started in AT-LEAST-ONCE mode
============================================================

[HH:MM:SS] Sent: 45 | Failed: 0 | Total: 50 (mode: at_least_once)
```

**Watch the Dashboard:**
- You'll see `Messages Received > Unique Events`
- **Duplicates detected!** Some event_ids appear multiple times
- This demonstrates duplication with `acks=all` + retries
- Retries are intentional to ensure delivery

**Run for ~30 seconds, then press Ctrl+C.**

---

#### **Mode 3: EXACTLY-ONCE (Idempotent + Transactional)**

Wait ~5 seconds for dashboard to clear, then:

```bash
python producer/producer.py exactly_once
```

**Watch the output:**
```
============================================================
Producer started in EXACTLY-ONCE mode
============================================================

[HH:MM:SS] Sent: 45 | Failed: 0 | Total: 50 (mode: exactly_once)
```

**Watch the Dashboard:**
- `Messages Received == Unique Events` (perfect 1:1!)
- **Zero duplicates**
- **Zero loss**
- This demonstrates exactly-once delivery with idempotence + transactions

**Run for ~30 seconds, then press Ctrl+C.**

---

## 🎯 Key Observations

Compare the three modes on the dashboard:

| Metric | At-Most-Once | At-Least-Once | Exactly-Once |
|--------|--------------|---------------|--------------|
| **Messages Received** | Lower (loss) | Higher (duplicates) | Exact match |
| **Duplicates** | 0 | Many | 0 |
| **Loss Rate** | High | 0% | 0% |
| **Accuracy** | Poor | OK | Perfect ✅ |

---

## 📊 What the Consumer/Dashboard Show

### **Consumer Console Output** (Terminal 4)
```
+-----+----------------+----------------+------------------+
| mode| unique_events  | total_received | duplicates_detected |
+-----+----------------+----------------+------------------+
|at_m_o|  45           |      42        |        0           |
|at_l_o|  45           |      52        |        7           |
|eo    |  45           |      45        |        0           |
+-----+----------------+----------------+------------------+
```

### **Dashboard Visual** (http://localhost:5000)
Three side-by-side cards showing:
- 🚨 **At-Most-Once:** Loss Rate, no duplicates
- 📋 **At-Least-Once:** Duplicate Rate, full delivery
- ✅ **Exactly-Once:** Perfect accuracy, no loss, no duplication

---

## 🧠 Deep Dive: Understanding the Kafka Configs

### **At-Most-Once Configuration**
```python
kafka_config = {
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  consumer/consumer_spark.py
    "retries": "0",        # Don't retry on failure
}
```
- Producer sends, doesn't wait
- If broker fails before writing, message is lost
- No retries means no duplicates possible

### **At-Least-Once Configuration**
```python
kafka_config = {
    "acks": "all",         # Wait for ALL in-sync replicas to ack
    "retries": "10",       # Retry 10 times on failure
    "max.in.flight.requests.per.connection": "5"
}
```
- Producer retries until acknowledged
- If retry succeeds but ack fails, duplicate happens
- Guarantees delivery but allows duplicates

### **Exactly-Once Configuration**
```python
kafka_config = {
    "acks": "all",                           # Wait for all replicas
    "enable.idempotence": "True",            # Dedup retried sends
    "transactional.id": "unique_producer_id" # Enable transactions
}
```
- Idempotent: Kafka deduplicates retried messages via producer sequence numbers
- Transactional: Atomic writes across partitions
- Guarantees exactly-once delivery

---

## 🔍 Duplicate Detection (How It Works)

Each message has a **unique `event_id`**:
```json
{
  "event_id": "AMO-1673289456321-45678",  // Unique identifier
  "mode": "at_least_once",
  "restaurant_id": "r1",
  "dish_id": "d5",
  ...
}
```

The **Spark Consumer** tracks:
```python
# For each event_id, count how many times it was received
duplicate_tracking = df_parsed \
    .groupBy("event_id", "mode") \
    .agg(count("*").alias("receive_count"))

# If receive_count > 1, it's a duplicate
is_duplicate = receive_count > 1
```

### **Example: At-Least-Once Mode**

Sent 10 unique events:
```
Event 1, Event 2, Event 3, Event 4, Event 5,
Event 6, Event 7, Event 8, Event 9, Event 10
```

Consumer receives (with duplicates):
```
Event 1, Event 2, Event 2 ← DUPLICATE!
Event 3, Event 4, Event 4 ← DUPLICATE!
Event 5, Event 6, Event 7, Event 8, Event 9, Event 10
```

**Metrics:**
- Sent: 10 unique events
- Received: 12 total messages
- Duplicates detected: 2
- Duplicate rate: 2/12 = 16.7%

This is **visible on the dashboard** in real-time!

---

## 📁 Project Structure

```
restaurant_demand_kafka/
├── README.md                    # This file
├── requirements.txt             # Python dependencies
├── producer/
│   └── producer.py             # Produces orders in 3 semantic modes
├── consumer/
│   └── consumer_spark.py        # Spark Streaming - detects duplicates
├── dashboard/
│   ├── app.py                  # Flask server - API endpoints
│   └── templates/
│       └── index.html          # Beautiful Kafka semantics dashboard
└── spark_model/
    └── train_model.py          # (Optional) ARIMA demand forecasting
```

### **Component Interaction**

```
┌─────────────┐
│  Producer   │  Sends orders in different semantic modes
└──────┬──────┘  (at_most_once / at_least_once / exactly_once)
       │ Kafka.orders
       ▼
┌──────────────────┐
│ Spark Consumer   │  Watches for duplicates using event_id
└──────┬───────────┘  Calculates metrics per mode
       │ Kafka.metrics
       ▼
┌──────────────────┐
│  Flask Dashboard │  http://localhost:5000
└──────────────────┘  Real-time visualization of semantics

---

## 🎯 Big‑data concepts covered

### **1. Kafka Event Streaming**
- Topics, partitions, consumer groups
- Bootstrap servers, replication
- Offset management and consumer lags
- How Kafka guarantees message ordering per partition

### **2. Delivery Semantics (The Core)**
- **At-Most-Once:** Fire-and-forget, lossy (`acks=0`)
- **At-Least-Once:** Persistent but duplicate-prone (`acks=all` with retries)
- **Exactly-Once:** Idempotent + transactional (perfect delivery)
- Tradeoffs between latency, throughput, and reliability
- Real-world scenarios for each semantic

### **3. Producer Idempotence**
- Sequence number tracking per producer
- How Kafka deduplicates retried messages
- `enable.idempotence` configuration
- Producer transactions and atomic multi-partition writes

### **4. Spark Structured Streaming**
- Reading from Kafka topics in streaming mode
- Windowed aggregations and watermarking
- State management for duplicate detection
- Writing streaming results back to Kafka

### **5. Real-Time Analytics**
- Live dashboards consuming Kafka data
- Metrics calculation in streaming mode
- Real-time monitoring of delivery characteristics
- Business intelligence from streaming events

---

## ❓ FAQ

### **Q: Why do I see duplicates in at_least_once mode?**
A: Producer retries on timeout/failure. If the broker crashes after writing but before sending ack, the producer retries. The message appears twice in Kafka.

### **Q: Why does at_most_once show lower counts?**
A: With `acks=0`, producer doesn't wait. If broker fails immediately, message is lost. Receiver never sees it.

### **Q: Is exactly_once guaranteed?**
A: Yes, but **per partition per transaction**. Exactly-once is the highest delivery guarantee Kafka offers.

### **Q: Can I see the Kafka topics in action?**
A: Yes! In Terminal 3, run:
```bash
cd ~/kafka_demo/kafka_2.13-3.4.0
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --topic orders --from-beginning | head -20
```

### **Q: My dashboard shows no data**
A: Check that:
1. Kafka topics exist: `bin/kafka-topics.sh --list --bootstrap-server localhost:9092`
2. Zookeeper is running (Terminal 1)
3. Kafka broker is running (Terminal 2)
4. Consumer is running and parsing JSON (Terminal 4)
5. Flask is running (Terminal 5)
6. Producer is running (Terminal 6)

### **Q: How do I stop everything?**
A: In each terminal, press `Ctrl+C`:
1. Zookeeper (Ctrl+C)
2. Kafka broker (Ctrl+C)
3. Spark consumer (Ctrl+C)
4. Flask dashboard (Ctrl+C)
5. Producer (Ctrl+C)

---

## 📚 Learning Outcomes

After running this demo, you'll understand:

✅ **What are Kafka delivery semantics?**  
The three fundamental guarantees (0-1, 1+, exactly 1) and their real-world implications.

✅ **Why duplicates happen**  
Producer retries can cause duplicates. This is **not a bug**, it's the tradeoff of at-least-once.

✅ **How exactly-once works**  
Idempotent producers + transactions enable perfect delivery without duplicates or loss.

✅ **Latency vs. Reliability**  
At-most-once is fast but lossy. Exactly-once is slower but perfect. Trade-offs matter.

✅ **Real-time metrics**  
How to measure delivery characteristics in production systems.

✅ **Spark Structured Streaming**  
Window aggregations, watermarking, and stream-to-stream processing in Kafka.

---

## 🔗 References

- [Apache Kafka Official Documentation](https://kafka.apache.org/documentation/)
- [Kafka Exactly-Once Semantics & Idempotent Producer](https://kafka.apache.org/documentation/#producerconfigs)
- [Kafka Delivery Semantics Deep Dive](https://kafka.apache.org/documentation/#semantics)
- [Confluent: Exactly-Once Semantics](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
- [Spark Structured Streaming + Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Confluent Kafka Python Client](https://docs.confluent.io/kafka-clients/python/current/overview.html)

---

## 🎓 Credits

This project was designed to make **Kafka delivery semantics tangible** through interactive demonstration. It's perfect for:
- Learning Kafka in detail
- Teaching others about delivery guarantees
- Interviewing engineers on system design
- Understanding real-world data streaming challenges

**Happy streaming! 🚀**  