# ⚡ Quick Start - Exact Commands (Step by Step)

## 🔧 STEP 0: One-Time Installation (Do This First!)

Run these commands **ONCE** to install dependencies:

```bash
# Update system
sudo apt update
sudo apt upgrade -y

# Install Java 11 (required for Kafka)
sudo apt install -y openjdk-11-jdk

# Verify Java installed
java -version

# Install Python packages
pip install pyspark==3.5.0 confluent-kafka==2.3.0 flask==3.0.0 pandas==2.0.3 statsmodels==0.14.0

# Create workspace and download Kafka 3.7.0
mkdir -p ~/kafka_demo
cd ~/kafka_demo
wget https://archive.apache.org/dist/kafka/3.7.0/kafka_2.13-3.7.0.tgz
tar -xzf kafka_2.13-3.7.0.tgz

# Verify extraction
ls -la kafka_2.13-3.7.0/
```

✅ **Installation complete!** Now follow the 6 terminals below.

---

## 🚀 STEP 1-6: Run These Commands (Each in a New Terminal)

### **TERMINAL 1: Start Zookeeper**

```bash
cd ~/kafka_2.13-3.7.0
bin/zookeeper-server-start.sh config/zookeeper.properties
```

**Expected Output:**
```
INFO Listening on /0.0.0.0:2181 (org.apache.zookeeper.server.ZooKeeperServer)
```

✅ **Leave this running. DO NOT CLOSE THIS TERMINAL.**

---

### **TERMINAL 2: Start Kafka Broker**

Open a **NEW** terminal:

```bash
 cd kafka_2.13-3.7.0
bin/kafka-server-start.sh config/server.properties
```

**Expected Output:**
```
INFO [KafkaServer id=0] started (kafka.server.KafkaServer)
```

✅ **Leave this running. DO NOT CLOSE THIS TERMINAL.**

---

### **TERMINAL 3: Create Kafka Topics**

Open a **THIRD** terminal:

```bash
cd ~/kafka_demo/kafka_2.13-3.7.0

# Create "orders" topic
bin/kafka-topics.sh --create \
  --topic orders \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Create "metrics" topic
bin/kafka-topics.sh --create \
  --topic metrics \
  --bootstrap-server localhost:9092 \
  --partitions 3 \
  --replication-factor 1

# Verify topics were created
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

**Expected Output:**
```
metrics
orders
```

✅ **You can close this terminal now. Topics are created.**

---

### **TERMINAL 4: Start Spark Consumer (Duplicate Detector)**

Open a **FOURTH** terminal:

```bash
cd ~/restaurant_demand_kafka
spark-submit \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.streaming.statefulOperator.checkCorrectness.enabled=false \
  consumer/consumer_spark.py
```

**Expected Output:**
```
======================================================================
KAFKA DELIVERY SEMANTICS CONSUMER (Spark Structured Streaming)
======================================================================
This consumer tracks:
  ✓ Messages received per mode
  ✓ Duplicates detected (same event_id multiple times)
  ✓ Message ordering
======================================================================

[1/4] Reading from Kafka 'orders' topic...
[2/4] Setting up duplicate detection via event_id...
[3/4] Starting real-time metrics streams...
[4/4] Forwarding metrics to 'metrics' topic for dashboard...
```

✅ **Leave this running. Watch for metric updates when producer starts.**

---

### **TERMINAL 5: Start Flask Dashboard**

Open a **FIFTH** terminal:

```bash
cd ~/restaurant_demand_kafka
python dashboard/app.py
```

**Expected Output:**
```
======================================================================
🌐 KAFKA DELIVERY SEMANTICS DASHBOARD
======================================================================
Starting Flask server on http://localhost:5000

Open browser and visit: http://localhost:5000
======================================================================
```

✅ **Now OPEN YOUR BROWSER and go to: http://localhost:5000**

You should see a beautiful dashboard with three colored cards (Red 🚨, Yellow 📋, Green ✅).

---

### **TERMINAL 6: Run Producer (Run Each Mode for 30 Seconds)**

Open a **SIXTH** terminal:

```bash
cd ~/restaurant_demand_kafka
```

#### **MODE 1: At-Most-Once (Fire-and-Forget - LOSS DEMO)**

```bash
python producer/producer.py at_most_once
```

**Expected Output:**
```
============================================================
Producer started in AT-MOST-ONCE mode
============================================================

[HH:MM:SS] Sent: 5 | Failed: 0 | Total: 5 (mode: at_most_once)
[HH:MM:SS] Sent: 10 | Failed: 0 | Total: 10 (mode: at_most_once)
[HH:MM:SS] Sent: 15 | Failed: 0 | Total: 15 (mode: at_most_once)
...
```

**Watch the Dashboard:**
- Messages Received will be **LESS** than Sent
- This shows **MESSAGE LOSS** (some messages lost!)
- Duplicates will be **0** (no retries, no duplication)

**Run for ~30 seconds, then press Ctrl+C:**
```
⏹  Producer interrupted.
📊 FINAL STATS:
   Total Attempted: XXX
   Successfully Sent: YYY
   Failed: 0
```

---

#### **MODE 2: At-Least-Once (Persistent - DUPLICATE DEMO)**

Wait 5 seconds for dashboard to clear, then run:

```bash
python producer/producer.py at_least_once
```

**Expected Output:**
```
============================================================
Producer started in AT-LEAST-ONCE mode
============================================================

[HH:MM:SS] Sent: 5 | Failed: 0 | Total: 5 (mode: at_least_once)
[HH:MM:SS] Sent: 10 | Failed: 0 | Total: 10 (mode: at_least_once)
...
```

**Watch the Dashboard:**
- Messages Received will be **MORE** than Unique Events
- This shows **DUPLICATES** (same event_id appearing multiple times!)
- Duplicate Rate will show ~6-8%

**Run for ~30 seconds, then press Ctrl+C.**

---

#### **MODE 3: Exactly-Once (Perfect Delivery)**

Wait 5 seconds for dashboard to clear, then run:

```bash
python producer/producer.py exactly_once
```

**Expected Output:**
```
============================================================
Producer started in EXACTLY-ONCE mode
============================================================

[HH:MM:SS] Sent: 5 | Failed: 0 | Total: 5 (mode: exactly_once)
[HH:MM:SS] Sent: 10 | Failed: 0 | Total: 10 (mode: exactly_once)
...
```

**Watch the Dashboard:**
- Messages Received = Unique Events
- Duplicates = 0
- Accuracy = 100% ✅
- This is **PERFECT DELIVERY** - exactly what was sent!

**Run for ~30 seconds, then press Ctrl+C.**

---

## 📊 Dashboard Comparison (What You Should See)

### **At-Most-Once Results**
```
🚨 At-Most-Once
Messages Received: 85
Unique Events: 85
Duplicates: 0
Loss Rate: 15% ← LOSS VISIBLE!
```

### **At-Least-Once Results**
```
📋 At-Least-Once
Messages Received: 107
Unique Events: 100
Duplicates: 7
Duplicate Rate: 6.5% ← DUPLICATES VISIBLE!
```

### **Exactly-Once Results**
```
✅ Exactly-Once
Messages Received: 100
Unique Events: 100
Duplicates: 0
Accuracy: 100% ← PERFECT! ✅
```

---

## 🎯 What Each Terminal Is Doing

| Terminal | Component | Purpose | Status |
|----------|-----------|---------|--------|
| **1** | Zookeeper | Cluster coordination | Keep running ▶️ |
| **2** | Kafka Broker | Message storage | Keep running ▶️ |
| **3** | Kafka CLI | Setup topics | Close after setup ✓ |
| **4** | Spark Consumer | Detects duplicates | Keep running ▶️ |
| **5** | Flask Web Server | Dashboard UI | Keep running ▶️ |
| **6** | Producer | Sends messages | Run each mode 30s |

---

## ⏱️ Timeline

```
0:00 - Start Zookeeper (Terminal 1)
0:05 - Start Kafka (Terminal 2)
0:10 - Create Topics (Terminal 3)
0:15 - Start Consumer (Terminal 4)
0:20 - Start Dashboard (Terminal 5)
0:25 - Open http://localhost:5000 in browser
0:30 - Run Producer at_most_once (Terminal 6) [30 sec]
1:00 - Switch to at_least_once [30 sec]
1:30 - Switch to exactly_once [30 sec]
2:00 - Done! ✅
```

---

## 🛑 Stopping Everything

When you're done, press **Ctrl+C** in each terminal:

```bash
# Terminal 1: Ctrl+C (Zookeeper)
# Terminal 2: Ctrl+C (Kafka)
# Terminal 4: Ctrl+C (Consumer)
# Terminal 5: Ctrl+C (Dashboard)
# Terminal 6: Ctrl+C (Producer) - already stopped
```

---

## ❓ Quick Troubleshooting

### **"Connection refused" when starting producer?**
- Make sure Zookeeper (Terminal 1) is running
- Make sure Kafka (Terminal 2) is running
- Wait 10 seconds and try again

### **Dashboard shows no data?**
- Verify Spark Consumer (Terminal 4) is running
- Check that producer is actively sending messages
- Verify Kafka broker is running (Terminal 2)
- Check browser at: http://localhost:5000

### **"Port already in use" error?**
Ports needed:
- **2181**: Zookeeper
- **9092**: Kafka
- **5000**: Flask Dashboard

Kill processes:
```bash
# Find and kill processes on these ports
sudo lsof -i :9092   # Kafka
sudo lsof -i :2181   # Zookeeper
sudo lsof -i :5000   # Flask
```

### **Kafka topics not created?**
Run this command in Terminal 3:
```bash
cd ~/kafka_demo/kafka_2.13-3.7.0
bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```

Should see:
```
metrics
orders
```

---

## 📖 Need More Info?

- **Detailed Setup?** → Read [README.md](README.md)
- **How It Works?** → Read [ARCHITECTURE.md](ARCHITECTURE.md)
- **Message Flow?** → Read [DATA_FLOW.md](DATA_FLOW.md)
- **Navigation?** → Read [INDEX.md](INDEX.md)

---

## 🎓 What You're Learning

After this demo, you'll understand:
- ✅ How Kafka delivery semantics work in practice
- ✅ Why some modes lose messages (at-most-once)
- ✅ Why some modes duplicate messages (at-least-once)
- ✅ How exactly-once prevents both loss and duplication
- ✅ When to use each semantic in production

---

**Total Time: ~2 hours** (mostly waiting for demo to run)

Ready? Start with **STEP 0: Installation** above! 🚀
