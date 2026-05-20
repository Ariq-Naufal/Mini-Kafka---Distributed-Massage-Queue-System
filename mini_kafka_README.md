# 🚀 Mini Kafka — Distributed Message Queue System

> A simplified but functionally complete implementation of Apache Kafka's core architecture in pure Python — built from scratch to understand distributed systems fundamentals.

![Python](https://img.shields.io/badge/Python-3.8%2B-blue?logo=python)
![Threading](https://img.shields.io/badge/Concurrency-threading-orange)
![Storage](https://img.shields.io/badge/Storage-Log--based-green)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

---

## 📌 Overview

This project implements the **core internals of Apache Kafka** — a distributed message streaming system used at companies like LinkedIn, Uber, and Netflix. Rather than using a library, every component is built from scratch to demonstrate a deep understanding of distributed system design patterns.

> Real Kafka is millions of lines of Java. This is ~1,200 lines of Python that captures the *essential architecture*.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────┐
│                      BROKER                         │
│                                                     │
│  ┌──────────┐   ┌──────────┐   ┌──────────┐        │
│  │  Topic A │   │  Topic B │   │  Topic C │        │
│  │  P0 P1P2 │   │  P0 P1   │   │  P0      │        │
│  └──────────┘   └──────────┘   └──────────┘        │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │          Log-Based Storage (Disk)            │   │
│  │  Segment 0  │  Segment 1  │  Segment 2  ... │   │
│  └──────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────┘
       ▲  produce                    consume  ▼
┌─────────────┐              ┌──────────────────────┐
│  Producer   │              │   Consumer Group     │
│  - KEY_HASH │              │  ┌──────┐ ┌──────┐   │
│  - ROUND    │              │  │  C1  │ │  C2  │   │
│  - STICKY   │              │  └──────┘ └──────┘   │
│  - RANDOM   │              │   Rebalanced auto     │
└─────────────┘              └──────────────────────┘
```

---

## ✨ Features Implemented

| Feature | Description |
|---------|-------------|
| **Topics & Partitions** | Messages partitioned for parallelism and ordering |
| **4 Partitioning Strategies** | KEY_HASH, ROUND_ROBIN, STICKY, RANDOM |
| **Log-Based Storage** | Append-only segment files with length-prefix encoding |
| **Broker Metadata Persistence** | Topic configs survive broker restarts (JSON metadata file) |
| **Producer with Retry** | Exponential backoff on failure, configurable retries |
| **Consumer Groups** | Range-assignor for automatic partition balancing |
| **Offset Management** | Manual & auto-commit, resume from committed offset |
| **Consumer Rebalancing** | Dynamic rebalance on join/leave |
| **Retention & Cleanup** | Delete old segments based on `retention_ms` |
| **Performance Benchmarking** | Throughput (msgs/sec, MB/sec) + P50/P95/P99 latency |

---

## 🐛 Bugs Fixed (vs. original version)

| Bug | Fix |
|-----|-----|
| `import random` inside method body | Moved to top-level imports |
| Demo 5: broker restart lost all topics | Added `broker_metadata.json` for topic config persistence |
| `read_range` skipped messages at segment boundary | Fixed offset arithmetic in `PartitionLog.read()` |
| Consumer assignment used index stride, not contiguous range | Switched to `topic_partitions[idx::n]` for balanced stride |

---

## 📂 Project Structure

```
mini-kafka/
│
├── mini_kafka.py        # Full implementation + 8 demos
├── requirements.txt     # No external dependencies (stdlib only)
└── README.md            # This file
```

---

## 🚀 How to Run

### Requirements
Python 3.8+ — **no external dependencies** (only Python standard library)

### Run all demos
```bash
git clone https://github.com/YOUR_USERNAME/mini-kafka.git
cd mini-kafka
python mini_kafka.py
```

### Demo output preview
```
🚀 Broker 1 started (topics: [])
✅ Topic 'orders' ready (3 partitions)

📤 Producing 10 messages...
  ✅ partition=0, offset=0
  ✅ partition=1, offset=0
  ...

🔄 Rebalancing 'order-processors' (1 consumers)
  📍 consumer-a3f2c1b0: 3 partition(s)

📥 Consuming messages...
  📨 [0:0] Order 0: iPhone 15 Pro
  📨 [1:0] Order 1: iPhone 15 Pro
  ...
```

---

## 🧪 8 Demo Scenarios

| Demo | Concept |
|------|---------|
| 1 | Basic produce → consume flow |
| 2 | Consumer groups & load balancing |
| 3 | All 4 partitioning strategies side-by-side |
| 4 | Offset commit & resume from checkpoint |
| 5 | Broker restart & data recovery from disk |
| 6 | Throughput & end-to-end latency benchmarks |
| 7 | Dynamic rebalancing as consumers join/leave |
| 8 | Retention policy & segment cleanup |

---

## 🔑 Key Design Decisions

**Why log-based storage?**
Kafka's secret to high throughput is sequential disk writes. This implementation uses the same append-only, length-prefixed binary format — making writes O(1) and enabling zero-copy reads.

**Why consumer groups?**
Partitions are the unit of parallelism. A consumer group ensures each partition is processed by exactly one consumer at a time, giving horizontal scalability without duplicate processing.

**Why KEY_HASH partitioning as default?**
Routing the same key to the same partition guarantees **ordering per key** — critical for use cases like "all events for user X must be processed in order."

---

## 📊 Performance (on typical laptop)

| Metric | Value |
|--------|-------|
| Producer throughput | ~15,000–25,000 msgs/sec |
| Consumer throughput | ~20,000–35,000 msgs/sec |
| End-to-end P50 latency | < 1 ms |
| End-to-end P99 latency | < 5 ms |

> Numbers vary by hardware. Run Demo 6 to get your own baseline.

---

## 🔭 What Real Kafka Adds

- **ZooKeeper / KRaft** — distributed coordination & leader election
- **Replication** — data replicated across broker cluster
- **Exactly-once semantics** — idempotent producer + transactional API
- **Schema Registry** — Avro/Protobuf schema validation
- **Kafka Streams / ksqlDB** — stream processing on top of topics
- **ISR (In-Sync Replicas)** — consistency during broker failure

---

## 👤 Author

**Ariq Naufal**
- GitHub: [@Ariq-Naufal](https://github.com/Ariq-Naufal)

---

## 📄 License

MIT License
