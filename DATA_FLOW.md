# 📊 Data Flow & Message Lifecycle

## Complete Message Journey Through the System

### Example Order Event

```json
{
  "event_id": "AMO-1673289456321-45678",
  "mode": "at_most_once",
  "restaurant_id": "r3",
  "dish_id": "d7",
  "quantity": 2,
  "timestamp": "2024-01-25T14:30:45.123Z",
  "producer_sent_at": 1673289456.321
}
```

---

## Scenario 1: At-Most-Once Mode

### Phase 1: Producer Generation
```
Step 1: Create unique event_id
        "AMO-1673289456321-45678"
        └─ Contains: mode, timestamp, random suffix

Step 2: Package as JSON
        {
          "event_id": "AMO-1673289456321-45678",
          "mode": "at_most_once",
          ...
        }

Step 3: Send with acks=0
        producer.produce(
          topic="orders",
          key="AMO-1673289456321-45678",
          value=json_bytes
        )
        └─ RETURNS IMMEDIATELY (no ack wait!)
```

### Phase 2: Kafka Broker

```
Broker receives message:
├─ Message arrives in partition 1 (key hash)
├─ Broker: "OK, I got it" (doesn't ack producer yet)
├─ Async write to disk
│
│ ⚠️ DANGER ZONE: Broker could crash here!
│
└─ Consumer reads from replicas
```

### Phase 3: Spark Consumer

```
Spark readStream (Kafka "orders" topic):
├─ Receives message
│  {
│    "event_id": "AMO-1673289456321-45678",
│    "mode": "at_most_once",
│    ...
│  }
├─ Parse JSON
├─ Track event_id seen
│  "at_most_once" -> {AMO-1673289456321-45678}
└─ Output metric: 1 received, 0 duplicates
```

### Phase 4: Dashboard Update

```
Kafka reads from "metrics" topic:
├─ Receives event
├─ Updates counter
│  at_most_once.total_received = 45
│  at_most_once.unique_events = 45
│  at_most_once.duplicates = 0
└─ Display on dashboard
```

### Result After 30 seconds:

```
Sent to Kafka:      100 messages
Actually persisted: ~85 messages
Consumer received:  ~85 unique events
Duplicates:         0
Loss detected:      15%
```

---

## Scenario 2: At-Least-Once Mode

### Phase 1: Producer with Retries

```
Step 1: Create unique event_id
        "ALO-1673289456421-45678"

Step 2: Send with acks=all, retries=10
        producer.produce(
          topic="orders",
          key="ALO-1673289456421-45678",
          value=json_bytes
        )
        └─ WAITS for acknowledgment
```

### Phase 2: Kafka Broker

```
First Send (t=0):
├─ Broker receives message
├─ Writes to disk ✓
├─ Replicates to all in-sync replicas ✓
├─ Sends ack to producer
│
│ ⚠️ NETWORK ISSUE: Ack packet lost!
│
└─ Producer: "Timeout waiting for ack!"

Retry 1 (t=1000ms):
├─ Producer resends same event_id
│  "ALO-1673289456421-45678" (again)
├─ Broker receives and writes again
│  (Broker has no dedup at this level!)
├─ Sends ack
└─ Producer: "Got it, done!"

Result in Kafka:
├─ Partition 1: [event_id="ALO-1673289456421-45678", v1]
├─ Partition 1: [event_id="ALO-1673289456421-45678", v2] ← DUPLICATE!
└─ Same content, different offsets
```

### Phase 3: Spark Consumer Duplicate Detection

```
Consumer reads both occurrences:

Message 1 (offset 1001):
{
  "event_id": "ALO-1673289456421-45678",
  "mode": "at_least_once",
  ...
}

Message 2 (offset 1002):
{
  "event_id": "ALO-1673289456421-45678",  ← SAME ID!
  "mode": "at_least_once",
  ...
}

Spark groups by event_id:
├─ ALO-1673289456421-45678 → receive_count: 2
├─ Detection: receive_count > 1 = DUPLICATE!
└─ Metric: 2 messages received, 1 unique, 1 duplicate
```

### Phase 4: Dashboard Update

```
Metrics accumulated over 30 seconds:

Kafka "orders" received: 107 messages
Unique event_ids:       100
Duplicates detected:    7 (from failed ack retries)
Duplicate rate:         7/107 = 6.5%

Dashboard shows:
├─ Messages Received: 107
├─ Unique Events: 100
├─ Duplicates: 7
└─ Duplicate Rate: 6.5% ⚠️
```

---

## Scenario 3: Exactly-Once Mode

### Phase 1: Idempotent Producer

```
Step 1: Producer initializes idempotence
        producer.enable.idempotence = true
        producer.transactional.id = "producer-xo-12345"

Step 2: Kafka assigns producer ID
        producer_id = 1, epoch = 1

Step 3: Create unique event_id + send
        "EO-1673289456521-45678"
        
        producer.begin_transaction()
        producer.produce(
          topic="orders",
          key="EO-1673289456521-45678",
          value=json_bytes,
          sequence=1  ← Kafka assigns this
        )
        producer.commit_transaction()
```

### Phase 2: Kafka Broker with Idempotence

```
First Send (t=0):
├─ Broker receives: [producer_id=1, seq=1, event_id="EO-..."]
├─ Writes to disk with sequence
├─ Records: "Highest seq from producer 1: 1"
├─ Sends ack
└─ Producer: "Done!"

Retry (network timeout, producer retries):
├─ Broker receives: [producer_id=1, seq=1, event_id="EO-..."]
│  ↓
│  Broker checks: "I've seen seq=1 from producer 1!"
│  ↓
│  "Skip duplicate, send ack anyway"
│
├─ Broker does NOT write again
├─ Sends ack
└─ Producer: "Done!"

Result in Kafka:
├─ Partition 1: [producer_id=1, seq=1, event_id="EO-1673289456521-45678"]
└─ (Only ONE occurrence in log!)
```

### Phase 3: Spark Consumer

```
Consumer reads messages:

Message 1 (offset 2001):
{
  "event_id": "EO-1673289456521-45678",
  "mode": "exactly_once",
  ...
}

(No duplicate! Broker dedup prevented it)

Spark groups by event_id:
├─ EO-1673289456521-45678 → receive_count: 1
├─ Detection: receive_count == 1 = NO DUPLICATE ✓
└─ Metric: 1 message received, 1 unique, 0 duplicates
```

### Phase 4: Dashboard Update

```
Metrics accumulated over 30 seconds:

Kafka "orders" received: 100 messages
Unique event_ids:       100
Duplicates detected:    0
Accuracy:               100%

Dashboard shows:
├─ Messages Received: 100 ✓
├─ Unique Events: 100 ✓
├─ Duplicates: 0 ✓
└─ Accuracy: 100% ✅
```

---

## Real-Time Comparison (30 Second Run)

### Terminal View (Spark Consumer)

```
Mode: at_most_once
├─ unique_events: 85
├─ total_received: 85
├─ duplicates_detected: 0
└─ loss_rate: 15%

Mode: at_least_once
├─ unique_events: 100
├─ total_received: 107
├─ duplicates_detected: 7
└─ duplicate_rate: 6.5%

Mode: exactly_once
├─ unique_events: 100
├─ total_received: 100
├─ duplicates_detected: 0
└─ accuracy: 100%
```

### Browser Dashboard (http://localhost:5000)

```
┌─────────────────────────────────────────────────────────────┐
│                                                             │
│  🚨 At-Most-Once       📋 At-Least-Once      ✅ Exactly-Once │
│  ─────────────         ─────────────────      ──────────── │
│  Recv: 85              Recv: 107              Recv: 100   │
│  Unique: 85            Unique: 100            Unique: 100 │
│  Dups: 0               Dups: 7                Dups: 0    │
│  Loss: 15% ❌          Dup Rate: 6.5% ⚠️      Acc: 100% ✅ │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

---

## Detailed Kafka Partition View

After running all 3 modes:

### Kafka "orders" Topic
```
Partition 0:
├─ [offset 1000] AMO-...: 1 received (at_most_once)
├─ [offset 1001] AMO-...: 1 received
├─ [offset 1002] AMO-...: 1 received
│  ... ~28 more at_most_once ...
├─ [offset 1030] ALO-...: 1 received (at_least_once)
├─ [offset 1031] ALO-...: 1 received (duplicate of 1030) ← SAME event_id
├─ [offset 1032] ALO-...: 1 received
│  ... ~30 more at_least_once (some duplicated) ...
├─ [offset 1070] EO-...: 1 received (exactly_once)
├─ [offset 1071] EO-...: 1 received
├─ [offset 1072] EO-...: 1 received
│  ... ~27 more exactly_once (NO duplicates) ...
└─ [offset 1099]

Total messages in partition: ~101
```

### Spark Dedup Logic
```python
events_by_id = {
    'AMO-xyz-001': {'count': 1, 'mode': 'at_most_once'},
    'AMO-xyz-002': {'count': 1, 'mode': 'at_most_once'},
    # ... ~25 more (but only ~85 sent, so 15 missing = LOSS)
    
    'ALO-abc-001': {'count': 1, 'mode': 'at_least_once'},
    'ALO-abc-002': {'count': 2, 'mode': 'at_least_once'},  ← DUPLICATE!
    'ALO-abc-003': {'count': 1, 'mode': 'at_least_once'},
    # ... 100 unique IDs, but 107 total = 7 DUPLICATES
    
    'EO-def-001': {'count': 1, 'mode': 'exactly_once'},
    'EO-def-002': {'count': 1, 'mode': 'exactly_once'},
    # ... 100 unique IDs, 100 total = PERFECT MATCH ✓
}
```

---

## Key Insights

1. **At-Most-Once:** No retries means **some never arrive**
   - Visible as: Received < Sent

2. **At-Least-Once:** Retries cause **duplicates in Kafka**
   - Visible as: Received > Unique Events
   - Consumer detects via event_id deduplication

3. **Exactly-Once:** Idempotence prevents **duplicates in Kafka**
   - Visible as: Received == Unique Events
   - Broker deduplicates using sequence numbers

---

## Monitoring in Real-Time

Watch the progression as each mode runs:

```
Time 0-30s: At-Most-Once
├─ Producer sends
├─ Consumer receives fewer (loss visible)
└─ Dashboard: Loss Rate increasing

Time 30-60s: At-Least-Once
├─ Producer sends with retries
├─ Consumer detects duplicates
└─ Dashboard: Duplicate Rate ~6-8%

Time 60-90s: Exactly-Once
├─ Producer sends with idempotence
├─ Consumer sees 1:1 mapping
└─ Dashboard: Accuracy 100% ✅
```

This demonstrates that **delivery semantics are not theoretical—they're observable in the data!**
