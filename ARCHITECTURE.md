# 📐 Architecture & Delivery Semantics Deep Dive

## System Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                     KAFKA CLUSTER (localhost:9092)              │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │  Topic: "orders" (3 partitions)                          │  │
│  │  - Receives messages from Producer                       │  │
│  │  - Stores: event_id, mode, restaurant_id, etc.         │  │
│  │  - Replicated for durability                            │  │
│  └──────────────────────────────────────────────────────────┘  │
│                              ▲                                   │
│                              │ (write)                           │
│  ┌────────────────────┐      │                                   │
│  │   Topic: "metrics" │      │                                   │
│  │  - Receives        │ ←────┘                                   │
│  │  - Duplicate       │                                          │
│  │  - Metrics data    │                                          │
│  └────────────────────┘                                          │
└─────────────────────────────────────────────────────────────────┘
        ▲                           ▲
        │                           │
  (produces)                  (consumes & analyzes)
        │                           │
   ┌────┴────┐        ┌────────────┴─────────┐
   │ Producer │        │  Spark Structured   │
   │ (3 modes)│        │     Streaming       │
   └──────────┘        │   Consumer          │
                       │ - Detects duplicates│
                       │ - Calculates metrics│
                       └────────┬────────────┘
                                │ (sends metrics to Kafka)
                                │
                       ┌────────┴────────┐
                       │  Flask Dashboard│
                       │  (reads metrics)│
                       │ http://5000     │
                       └─────────────────┘
```

---

## Deep Dive: How Delivery Semantics Work

### **1️⃣ At-Most-Once Semantic**

#### Configuration
```python
{
    "acks": "0",           # Don't wait for acknowledgment
    "retries": "0"         # Don't retry on failure
}
```

#### Flow
```
Producer                          Kafka Broker
   │                                  │
   ├──> Send message ─────────────────>│
   │                                   │  (no ack needed!)
   └─ Return immediately              │
                                       │  (writing to disk asynchronously)
                                       │
    [If broker crashes here ─────────> LOST!]
```

#### Why Loss Happens
1. Producer sends message with `acks=0`
2. Producer returns **immediately** without waiting for broker response
3. If broker crashes before writing to disk → message is lost
4. Producer never retries (retries=0)
5. Consumer never sees the message

#### Metrics You'll See
```
Messages Sent:    100
Messages Received: ~85 (some lost)
Duplicates:       0 (no retries)
Loss Rate:        15%
```

#### When to Use
- Application metrics (losing 1-2% is OK)
- Log aggregation (high volume, low importance)
- Real-time analytics where approximation is acceptable

---

### **2️⃣ At-Least-Once Semantic**

#### Configuration
```python
{
    "acks": "all",         # Wait for ALL in-sync replicas
    "retries": "10",       # Retry 10 times on failure
    "max.in.flight.requests.per.connection": "5"
}
```

#### Flow
```
Producer                          Kafka Broker
   │                                  │
   ├──> Send message ─────────────────>│
   │                                   │ (write to all replicas)
   │                                   │ 
   │    <─────── Ack ─────────────────<│
   │                                   │
   └─ Return SUCCESS                  │
```

#### Why Duplicates Can Happen
```
Scenario: Broker acks but then crashes

1. Producer sends message
2. Broker writes to disk ✓
3. Broker sends ack
4. Network issue: ack lost
5. Producer timeout, retries
6. Broker writes again (duplicate!)
7. Producer gets ack
```

The message is now in Kafka **twice**, but consumer sees **one** of them as a duplicate!

#### Metrics You'll See
```
Messages Sent:     100
Messages Received: ~107 (some duplicated)
Unique Events:     100
Duplicates:        7 (from retries)
Duplicate Rate:    7%
```

#### Detection in Spark
```python
# Group by event_id to detect duplicates
df_parsed.groupBy("event_id", "mode").agg(
    count("*").alias("receive_count")  # Should be 1 for no duplication
).filter(col("receive_count") > 1)     # Shows duplicates
```

#### When to Use
- Financial transactions (with deduplication)
- Order processing (deduplicate downstream)
- Any critical data where loss > duplication

---

### **3️⃣ Exactly-Once Semantic**

#### Configuration
```python
{
    "acks": "all",                           # Wait for all replicas
    "retries": "10",                         # Retry on failure
    "enable.idempotence": True,              # Enable deduplication
    "transactional.id": "unique_producer_id" # Enable transactions
}
```

#### How Idempotence Works
```
Producer sends sequence numbers with each message:

Message 1: [seq=1, data="order1"]
Message 2: [seq=2, data="order2"]
Message 3: [seq=3, data="order3"]

Producer retries Message 2:
   [seq=2, data="order2"]  <- Same sequence number!

Kafka Broker recognizes:
   "I've already written seq=2, skip duplicate"
   
Result: Exactly one copy in Kafka ✓
```

#### How Transactions Work
```
Producer.begin_transaction()
  ├─ Writes message to partition 1: [seq=1, data]
  ├─ Writes message to partition 2: [seq=2, data]
  ├─ Writes message to partition 3: [seq=3, data]
Producer.commit_transaction()
  └─ Atomically commits all 3 writes
     (or rolls back all 3 if any fail)
```

**Guarantee:** All-or-nothing writes across partitions.

#### Metrics You'll See
```
Messages Sent:     100
Messages Received: 100 (perfect!)
Unique Events:     100
Duplicates:        0 (deduped)
Accuracy:          100%
```

#### When to Use
- **CRITICAL:** Payments, refunds
- Inventory updates (can't afford loss or duplication)
- Legal/compliance records
- Database master records

---

## Implementation Deep Dive

### Producer Code (Simplified)

```python
# At-Most-Once
producer = Producer({
    "acks": "0",
    "retries": 0
})
producer.produce("orders", value=json_str)
# Returns immediately, no ack

# At-Least-Once
producer = Producer({
    "acks": "all",
    "retries": 10
})
producer.produce("orders", value=json_str)
# Retries until all in-sync replicas ack

# Exactly-Once
producer = Producer({
    "acks": "all",
    "enable.idempotence": True,
    "transactional.id": "tx1"
})
producer.init_transactions()
producer.begin_transaction()
producer.produce("orders", value=json_str)
producer.commit_transaction()
# Atomic transaction with deduplication
```

### Consumer Code (Spark)

The consumer detects duplicates by grouping on `event_id`:

```python
# Parse messages from Kafka
df = spark.readStream.format("kafka").load()
parsed = df.select(from_json(col("value"), schema).alias("data"))

# Detect duplicates: each event_id should appear once
duplicates = parsed.groupBy("event_id", "mode").agg(
    count("*").alias("receive_count"),
    col("mode")
).filter(col("receive_count") > 1)

# Calculate metrics
metrics = duplicates.groupBy("mode").agg(
    countDistinct("event_id").alias("unique_events"),
    sum("receive_count").alias("total_received"),
    (sum("receive_count") - countDistinct("event_id")).alias("duplicates_detected")
)
```

**Key Insight:** We detect duplicates by seeing the **same event_id multiple times**. Each message has a unique ID that producer generates.

---

## Kafka Broker Behavior

### What Happens Inside Kafka

```
AT-MOST-ONCE: acks=0
├─ Producer sends
├─ Broker: "OK I got it" (but doesn't write yet)
├─ Producer: "Great, done!"
└─ [Broker crash] → Message lost

AT-LEAST-ONCE: acks=all + retries
├─ Producer sends
├─ Broker: writes to all replicas, then sends ack
├─ Producer: "Got the ack, done!"
└─ [Broker crash] → message already persisted

EXACTLY-ONCE: idempotent + transactions
├─ Producer sends: [seq=1, data]
├─ Broker: detects new sequence number, writes
├─ Producer: "Got the ack, done!"
├─ [Producer retries same message: seq=1]
├─ Broker: "I've seen seq=1, skipping duplicate"
└─ Result: single message in log
```

---

## Dashboard Interpretation

### Reading the Metrics

When you see on the dashboard:

```
At-Most-Once:
├─ Messages Received: 85
├─ Unique Events: 85
├─ Duplicates: 0
└─ Loss Rate: 15%

→ INTERPRETATION: 15 messages were lost in transit
```

```
At-Least-Once:
├─ Messages Received: 107
├─ Unique Events: 100
├─ Duplicates: 7
└─ Duplicate Rate: 7%

→ INTERPRETATION: 7 event_ids appeared twice
                  Indicates retries occurred
```

```
Exactly-Once:
├─ Messages Received: 100
├─ Unique Events: 100
├─ Duplicates: 0
└─ Accuracy: 100%

→ INTERPRETATION: Perfect delivery, exactly what was sent
```

---

## Performance Implications

```
Latency (lower is better):
  At-Most-Once:  █░░░░░░░░░  ~1ms   (immediate return)
  At-Least-Once: ░░░░██░░░░  ~10ms  (wait for acks)
  Exactly-Once:  ░░░░░░░██░░ ~20ms  (transactions)

Throughput (higher is better):
  At-Most-Once:  ████████░░  100k msg/s
  At-Least-Once: ██████░░░░  60k msg/s
  Exactly-Once:  ████░░░░░░  30k msg/s

Reliability (higher is better):
  At-Most-Once:  ██░░░░░░░░  May lose data
  At-Least-Once: ████████░░  No loss, but duplicates
  Exactly-Once:  ██████████  Perfect guarantee
```

---

## Testing in Production

### How to Verify Each Semantic

**For At-Most-Once:**
```bash
# Kill broker during producer send
# Watch dashboard: some messages never appear
```

**For At-Least-Once:**
```bash
# Run producer with slow network (simulate latency)
# Watch: some event_ids appear multiple times
```

**For Exactly-Once:**
```bash
# Even with network issues, retries
# Watch: event_ids appear exactly once
```

---

## Real-World Scenarios

| Scenario | Recommended | Why |
|----------|-------------|-----|
| **Mobile app events** | At-Most-Once | Loss acceptable, need speed |
| **User session tracking** | At-Least-Once | Can deduplicate on key |
| **E-commerce orders** | Exactly-Once | **Cannot lose or duplicate** |
| **Payment transactions** | Exactly-Once | Critical, no loss allowed |
| **Metrics/monitoring** | At-Most-Once | Approximation OK |
| **API call logs** | At-Least-Once | Loss bad, dup OK |
| **Inventory management** | Exactly-Once | Must be precise |

---

## Further Reading

1. **Idempotence:** How Kafka deduplicates producer retries
   - Uses producer ID + sequence numbers
   - Broker tracks highest seen sequence per partition

2. **Transactions:** How exactly-once across partitions works
   - Coordinator ensures atomicity
   - All-or-nothing commits

3. **Replication:** How "acks=all" ensures durability
   - In-sync replica set (ISR)
   - Min.insync.replicas configuration

---

## Summary Table

| Aspect | At-Most-Once | At-Least-Once | Exactly-Once |
|--------|--|--|--|
| **acks** | 0 | all | all |
| **retries** | 0 | 10+ | 10+ |
| **idempotence** | ❌ | ❌ | ✅ |
| **transactions** | ❌ | ❌ | ✅ |
| **Loss** | ❌ Possible | ✅ No | ✅ No |
| **Duplicates** | ✅ No | ❌ Possible | ✅ No |
| **Latency** | ⚡ Fast | 🐌 Medium | 🐢 Slow |
| **Use Case** | Metrics | Orders | Payments |

---

This architecture ensures that **delivery semantics are not just theory, but visually observable reality**.
