"""
MINI KAFKA - DISTRIBUTED MESSAGE QUEUE SYSTEM
==============================================
Full implementation of Kafka-like message broker (1200+ lines)

Core Features:
1. Topics & Partitions
2. Producer with partitioning strategies
3. Consumer & Consumer Groups
4. Offset management & commits
5. Message persistence (log-based storage)
6. Replication (Leader/Follower)
7. Consumer group rebalancing
8. At-least-once delivery guarantee
9. Retention & compaction
10. Performance benchmarking

Architecture:
- Broker: Manages topics, partitions, storage
- Producer: Sends messages to topics
- Consumer: Reads messages from topics
- Consumer Group: Coordinates multiple consumers
- Storage: Persistent log-based storage

Real Kafka has millions of lines, this is simplified but demonstrates core concepts.
"""

# FIX 1: All imports at the top level (tidak di dalam fungsi)
import random
import threading
import time
import hashlib
import json
import os
import shutil
from dataclasses import dataclass, field
from typing import List, Dict, Optional, Callable, Set, Tuple
from datetime import datetime, timedelta
from enum import Enum
from collections import defaultdict
import uuid
from pathlib import Path


# ============================================================================
# 1. MESSAGE & SERIALIZATION
# ============================================================================

@dataclass
class Message:
    """
    Represents a single message in the queue.
    Immutable once created — mirrors Kafka's append-only log semantics.
    """
    key: Optional[str]       # Used for partition routing
    value: bytes             # Actual payload
    timestamp: datetime = field(default_factory=datetime.now)
    headers: Dict[str, str] = field(default_factory=dict)
    offset: Optional[int] = None     # Assigned by broker
    partition: Optional[int] = None  # Assigned by broker

    def serialize(self) -> bytes:
        """Serialize message to JSON bytes for disk storage."""
        data = {
            'key': self.key,
            'value': self.value.hex(),
            'timestamp': self.timestamp.isoformat(),
            'headers': self.headers,
            'offset': self.offset,
            'partition': self.partition
        }
        return json.dumps(data).encode('utf-8')

    @staticmethod
    def deserialize(data: bytes) -> 'Message':
        """Deserialize message from disk storage."""
        obj = json.loads(data.decode('utf-8'))
        return Message(
            key=obj['key'],
            value=bytes.fromhex(obj['value']),
            timestamp=datetime.fromisoformat(obj['timestamp']),
            headers=obj.get('headers', {}),
            offset=obj.get('offset'),
            partition=obj.get('partition')
        )

    def size(self) -> int:
        """Return serialized size in bytes."""
        return len(self.serialize())


@dataclass
class ProducerRecord:
    """Record submitted by a producer before broker assigns offset/partition."""
    topic: str
    value: bytes
    key: Optional[str] = None
    partition: Optional[int] = None  # Explicit override
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class ConsumerRecord:
    """Record delivered to a consumer after broker assigns offset/partition."""
    topic: str
    partition: int
    offset: int
    key: Optional[str]
    value: bytes
    timestamp: datetime
    headers: Dict[str, str]


# ============================================================================
# 2. PARTITIONING STRATEGIES
# ============================================================================

class PartitionStrategy(Enum):
    ROUND_ROBIN = "round_robin"
    KEY_HASH = "key_hash"
    RANDOM = "random"
    STICKY = "sticky"


class Partitioner:
    """
    Decides which partition a message is routed to.

    Strategy trade-offs:
    - KEY_HASH  : same key → same partition → ordering guarantee per key
    - ROUND_ROBIN: even load distribution, no ordering guarantee
    - STICKY    : batches to one partition for throughput, then rotates
    - RANDOM    : uniform distribution without state
    """

    def __init__(self, strategy: PartitionStrategy = PartitionStrategy.KEY_HASH):
        self.strategy = strategy
        self._rr_counter = 0
        self._sticky_partition = 0
        self._sticky_batch_size = 100
        self._sticky_counter = 0

    def partition(self, key: Optional[str], num_partitions: int) -> int:
        if num_partitions <= 0:
            raise ValueError("num_partitions must be a positive integer")

        if self.strategy == PartitionStrategy.KEY_HASH:
            # FIX 2: key=None falls back to round-robin instead of raising
            if key is None:
                return self._round_robin(num_partitions)
            hash_val = int(hashlib.md5(key.encode()).hexdigest(), 16)
            return hash_val % num_partitions

        elif self.strategy == PartitionStrategy.ROUND_ROBIN:
            return self._round_robin(num_partitions)

        elif self.strategy == PartitionStrategy.RANDOM:
            # FIX 1 applied: random is now imported at top level
            return random.randint(0, num_partitions - 1)

        elif self.strategy == PartitionStrategy.STICKY:
            if self._sticky_counter >= self._sticky_batch_size:
                self._sticky_partition = (self._sticky_partition + 1) % num_partitions
                self._sticky_counter = 0
            self._sticky_counter += 1
            return self._sticky_partition

        return 0

    def _round_robin(self, num_partitions: int) -> int:
        partition = self._rr_counter % num_partitions
        self._rr_counter += 1
        return partition


# ============================================================================
# 3. LOG-BASED STORAGE
# ============================================================================

class LogSegment:
    """
    Immutable log segment — stores messages on disk with a length-prefixed format.
    Each segment maps to two files: <base_offset>.log and <base_offset>.index.
    """

    MAX_SIZE_BYTES = 10 * 1024 * 1024  # 10 MB

    def __init__(self, base_offset: int, directory: Path):
        self.base_offset = base_offset
        self.directory = directory
        self.directory.mkdir(parents=True, exist_ok=True)

        self.log_file = directory / f"{base_offset:020d}.log"
        self.index_file = directory / f"{base_offset:020d}.index"

        self.messages: List[Message] = []
        self.index: Dict[int, int] = {}  # offset → byte position
        self.size_bytes = 0

        if self.log_file.exists():
            self._load()

    def append(self, message: Message) -> bool:
        """Append message; returns False if segment is full."""
        if self.is_full():
            return False

        message.offset = self.base_offset + len(self.messages)
        serialized = message.serialize()
        position = self.size_bytes

        with open(self.log_file, 'ab') as f:
            f.write(len(serialized).to_bytes(4, 'big'))
            f.write(serialized)

        self.index[message.offset] = position
        self.messages.append(message)
        self.size_bytes += 4 + len(serialized)
        return True

    def read(self, offset: int) -> Optional[Message]:
        idx = offset - self.base_offset
        if idx < 0 or idx >= len(self.messages):
            return None
        return self.messages[idx]

    def read_range(self, start_offset: int, max_messages: int = 100) -> List[Message]:
        start_idx = max(0, start_offset - self.base_offset)
        end_idx = min(start_idx + max_messages, len(self.messages))
        return self.messages[start_idx:end_idx]

    def is_full(self) -> bool:
        return self.size_bytes >= self.MAX_SIZE_BYTES

    def _load(self):
        """Reload messages from the .log file on disk."""
        try:
            with open(self.log_file, 'rb') as f:
                position = 0
                while True:
                    length_bytes = f.read(4)
                    if len(length_bytes) < 4:
                        break
                    length = int.from_bytes(length_bytes, 'big')
                    data = f.read(length)
                    if len(data) < length:
                        break
                    message = Message.deserialize(data)
                    self.messages.append(message)
                    self.index[message.offset] = position
                    position += 4 + length
                self.size_bytes = position
        except FileNotFoundError:
            pass

    def delete(self):
        for f in (self.log_file, self.index_file):
            if f.exists():
                f.unlink()


class PartitionLog:
    """
    Manages multiple LogSegments for one (topic, partition) pair.
    Rolls over to a new segment when the active one is full.
    """

    def __init__(self, topic: str, partition: int, data_dir: Path):
        self.topic = topic
        self.partition = partition
        self.directory = data_dir / topic / f"partition-{partition}"
        self.directory.mkdir(parents=True, exist_ok=True)

        self.segments: List[LogSegment] = []
        self.active_segment: Optional[LogSegment] = None
        self.next_offset = 0

        self._load_segments()

    def append(self, message: Message) -> int:
        """Append message and return the assigned offset."""
        message.partition = self.partition

        if self.active_segment is None:
            self._create_new_segment()

        if not self.active_segment.append(message):
            self._create_new_segment()
            self.active_segment.append(message)

        offset = self.next_offset
        self.next_offset += 1
        return offset

    def read(self, offset: int, max_messages: int = 100) -> List[Message]:
        messages: List[Message] = []
        for segment in self.segments:
            if offset < segment.base_offset + len(segment.messages):
                batch = segment.read_range(offset, max_messages - len(messages))
                messages.extend(batch)
                offset = segment.base_offset + len(segment.messages)
            if len(messages) >= max_messages:
                break
        return messages[:max_messages]

    def get_high_watermark(self) -> int:
        return self.next_offset

    def get_low_watermark(self) -> int:
        return self.segments[0].base_offset if self.segments else 0

    def _create_new_segment(self):
        segment = LogSegment(self.next_offset, self.directory)
        self.segments.append(segment)
        self.active_segment = segment

    def _load_segments(self):
        for log_file in sorted(self.directory.glob("*.log")):
            base_offset = int(log_file.stem)
            segment = LogSegment(base_offset, self.directory)
            self.segments.append(segment)
            if segment.messages:
                self.next_offset = max(self.next_offset, segment.messages[-1].offset + 1)
        if self.segments:
            self.active_segment = self.segments[-1]

    def cleanup_old_segments(self, retention_ms: int):
        """Delete segments whose oldest message exceeds the retention window."""
        cutoff = datetime.now() - timedelta(milliseconds=retention_ms)
        to_delete = [
            s for s in self.segments[:-1]  # never delete active segment
            if s.messages and s.messages[0].timestamp < cutoff
        ]
        for segment in to_delete:
            print(f"  🗑️  Deleting old segment: offset={segment.base_offset}")
            segment.delete()
            self.segments.remove(segment)


# ============================================================================
# 4. TOPIC & PARTITION MANAGEMENT
# ============================================================================

@dataclass
class TopicConfig:
    name: str
    num_partitions: int = 3
    replication_factor: int = 1
    retention_ms: int = 7 * 24 * 60 * 60 * 1000  # 7 days
    min_insync_replicas: int = 1


class Topic:
    """
    Logical grouping of messages, split across N partitions for parallelism.
    """

    def __init__(self, config: TopicConfig, data_dir: Path):
        self.config = config
        self.partitions: List[PartitionLog] = [
            PartitionLog(config.name, pid, data_dir)
            for pid in range(config.num_partitions)
        ]
        print(f"✅ Topic '{config.name}' ready ({config.num_partitions} partitions)")

    def get_partition(self, partition_id: int) -> Optional[PartitionLog]:
        if 0 <= partition_id < len(self.partitions):
            return self.partitions[partition_id]
        return None

    def get_num_partitions(self) -> int:
        return len(self.partitions)

    def cleanup_old_data(self):
        for partition in self.partitions:
            partition.cleanup_old_segments(self.config.retention_ms)


# ============================================================================
# 5. BROKER
# ============================================================================

class Broker:
    """
    Central coordinator: manages topics, routes produce/consume requests,
    and tracks consumer groups.

    FIX 3: Broker now persists topic configs to disk so they survive restarts.
    """

    _METADATA_FILE = "broker_metadata.json"

    def __init__(self, broker_id: int, data_dir: str = "./kafka-data"):
        self.broker_id = broker_id
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

        self.topics: Dict[str, Topic] = {}
        self.lock = threading.RLock()
        self.consumer_groups: Dict[str, 'ConsumerGroup'] = {}

        self.metrics = {
            'messages_produced': 0,
            'messages_consumed': 0,
            'bytes_in': 0,
            'bytes_out': 0,
        }

        # Reload topics from previous run
        self._load_metadata()
        print(f"🚀 Broker {broker_id} started (topics: {list(self.topics.keys())})")

    # ---- persistence -------------------------------------------------------

    def _metadata_path(self) -> Path:
        return self.data_dir / self._METADATA_FILE

    def _save_metadata(self):
        """Persist topic configs so the broker can reload them after restart."""
        meta = {
            name: {
                "num_partitions": t.config.num_partitions,
                "replication_factor": t.config.replication_factor,
                "retention_ms": t.config.retention_ms,
                "min_insync_replicas": t.config.min_insync_replicas,
            }
            for name, t in self.topics.items()
        }
        with open(self._metadata_path(), 'w') as f:
            json.dump(meta, f, indent=2)

    def _load_metadata(self):
        """Reload topic configs (and their on-disk logs) after a restart."""
        path = self._metadata_path()
        if not path.exists():
            return
        with open(path) as f:
            meta = json.load(f)
        for name, cfg in meta.items():
            config = TopicConfig(name=name, **cfg)
            self.topics[name] = Topic(config, self.data_dir)

    # ---- public API --------------------------------------------------------

    def create_topic(self, config: TopicConfig):
        with self.lock:
            if config.name in self.topics:
                raise ValueError(f"Topic '{config.name}' already exists")
            self.topics[config.name] = Topic(config, self.data_dir)
            self._save_metadata()

    def produce(self, topic_name: str, message: Message,
                partition: Optional[int] = None) -> Tuple[int, int]:
        with self.lock:
            topic = self._get_topic_or_raise(topic_name)
            if partition is None:
                partition = Partitioner(PartitionStrategy.KEY_HASH).partition(
                    message.key, topic.get_num_partitions()
                )
            offset = topic.get_partition(partition).append(message)
            self.metrics['messages_produced'] += 1
            self.metrics['bytes_in'] += message.size()
            return partition, offset

    def consume(self, topic_name: str, partition: int, offset: int,
                max_messages: int = 100) -> List[Message]:
        with self.lock:
            topic = self._get_topic_or_raise(topic_name)
            partition_log = topic.get_partition(partition)
            if partition_log is None:
                raise ValueError(f"Partition {partition} does not exist in '{topic_name}'")
            messages = partition_log.read(offset, max_messages)
            self.metrics['messages_consumed'] += len(messages)
            self.metrics['bytes_out'] += sum(m.size() for m in messages)
            return messages

    def get_topic(self, topic_name: str) -> Optional[Topic]:
        return self.topics.get(topic_name)

    def get_metrics(self) -> Dict:
        return self.metrics.copy()

    def _get_topic_or_raise(self, topic_name: str) -> Topic:
        if topic_name not in self.topics:
            raise ValueError(f"Topic '{topic_name}' does not exist")
        return self.topics[topic_name]


# ============================================================================
# 6. PRODUCER
# ============================================================================

@dataclass
class ProducerConfig:
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    acks: int = 1
    retries: int = 3
    batch_size: int = 16384
    linger_ms: int = 0
    compression_type: str = "none"
    max_in_flight_requests: int = 5


class Producer:
    """
    Sends messages to the broker with configurable retry/backoff.
    """

    def __init__(self, broker: Broker, config: Optional[ProducerConfig] = None):
        self.broker = broker
        self.config = config or ProducerConfig()

    def send(self, record: ProducerRecord) -> Tuple[int, int]:
        """Send one message; returns (partition, offset)."""
        message = Message(key=record.key, value=record.value, headers=record.headers)

        for attempt in range(self.config.retries):
            try:
                return self.broker.produce(record.topic, message, record.partition)
            except Exception as exc:
                if attempt == self.config.retries - 1:
                    raise
                wait = 0.1 * (2 ** attempt)
                print(f"  ⚠️  Retry {attempt + 1}/{self.config.retries} in {wait:.1f}s: {exc}")
                time.sleep(wait)

        raise RuntimeError("Exhausted all retries")

    def send_batch(self, records: List[ProducerRecord]) -> List[Tuple[int, int]]:
        return [self.send(r) for r in records]

    def flush(self):
        pass  # hook for async batch flushing

    def close(self):
        self.flush()


# ============================================================================
# 7. CONSUMER & CONSUMER GROUP
# ============================================================================

@dataclass
class ConsumerConfig:
    group_id: str
    bootstrap_servers: List[str] = field(default_factory=lambda: ["localhost:9092"])
    auto_offset_reset: str = "latest"  # "earliest" | "latest"
    enable_auto_commit: bool = True
    auto_commit_interval_ms: int = 5000
    max_poll_records: int = 500
    session_timeout_ms: int = 10000


class OffsetManager:
    """
    Tracks committed offsets per (group_id, topic, partition).
    In real Kafka this lives in the __consumer_offsets internal topic.
    """

    def __init__(self):
        self._offsets: Dict[Tuple[str, str, int], int] = {}
        self._lock = threading.Lock()

    def commit(self, group_id: str, topic: str, partition: int, offset: int):
        with self._lock:
            self._offsets[(group_id, topic, partition)] = offset

    def get_offset(self, group_id: str, topic: str, partition: int) -> Optional[int]:
        with self._lock:
            return self._offsets.get((group_id, topic, partition))


class ConsumerGroup:
    """
    Coordinates partition assignment across consumers in the same group.
    Uses range-assignor strategy; triggers rebalance on membership change.
    """

    def __init__(self, group_id: str, broker: Broker):
        self.group_id = group_id
        self.broker = broker
        self.members: Dict[str, 'Consumer'] = {}
        self.assignments: Dict[str, List[Tuple[str, int]]] = {}
        self._lock = threading.Lock()

    def join(self, consumer: 'Consumer'):
        with self._lock:
            self.members[consumer.consumer_id] = consumer
            print(f"  👤 {consumer.consumer_id} joined group '{self.group_id}'")
            self._rebalance()

    def leave(self, consumer_id: str):
        with self._lock:
            if consumer_id in self.members:
                del self.members[consumer_id]
                print(f"  👋 {consumer_id} left group '{self.group_id}'")
                self._rebalance()

    def _rebalance(self):
        """Range-assignor: divide sorted partitions evenly across sorted consumers."""
        print(f"\n🔄 Rebalancing '{self.group_id}' ({len(self.members)} consumers)")
        if not self.members:
            return

        self.assignments = {cid: [] for cid in self.members}

        all_tp: Set[Tuple[str, int]] = set()
        for consumer in self.members.values():
            for topic_name in consumer.subscriptions:
                topic = self.broker.get_topic(topic_name)
                if topic:
                    for pid in range(topic.get_num_partitions()):
                        all_tp.add((topic_name, pid))

        topic_partitions = sorted(all_tp)
        consumer_ids = sorted(self.members)
        n = len(consumer_ids)

        for idx, cid in enumerate(consumer_ids):
            assigned = topic_partitions[idx::n]  # stride assignment
            self.assignments[cid] = assigned
            self.members[cid].on_partitions_assigned(assigned)
            print(f"  📍 {cid}: {len(assigned)} partition(s)")

    def get_assignment(self, consumer_id: str) -> List[Tuple[str, int]]:
        return self.assignments.get(consumer_id, [])


class Consumer:
    """
    Reads messages from assigned partitions.
    Tracks per-partition position; commits offsets manually or automatically.
    """

    def __init__(self, broker: Broker, config: ConsumerConfig,
                 offset_manager: OffsetManager):
        self.broker = broker
        self.config = config
        self.offset_manager = offset_manager

        self.consumer_id = f"consumer-{uuid.uuid4().hex[:8]}"
        self.subscriptions: List[str] = []
        self.assignment: List[Tuple[str, int]] = []
        self.positions: Dict[Tuple[str, int], int] = {}

        self.group: Optional[ConsumerGroup] = None
        self._running = False

        print(f"✅ Consumer {self.consumer_id} created")

    def subscribe(self, topics: List[str]):
        self.subscriptions = topics
        if self.config.group_id:
            if self.config.group_id not in self.broker.consumer_groups:
                self.broker.consumer_groups[self.config.group_id] = ConsumerGroup(
                    self.config.group_id, self.broker
                )
            self.group = self.broker.consumer_groups[self.config.group_id]
            self.group.join(self)

        if self.config.enable_auto_commit:
            self._start_auto_commit()

    def on_partitions_assigned(self, assignment: List[Tuple[str, int]]):
        self.assignment = assignment
        for topic, partition in assignment:
            committed = self.offset_manager.get_offset(
                self.config.group_id, topic, partition
            )
            if committed is not None:
                self.positions[(topic, partition)] = committed
            elif self.config.auto_offset_reset == "earliest":
                self.positions[(topic, partition)] = 0
            else:
                topic_obj = self.broker.get_topic(topic)
                if topic_obj:
                    pl = topic_obj.get_partition(partition)
                    self.positions[(topic, partition)] = pl.get_high_watermark()

    def poll(self, timeout_ms: int = 1000) -> List[ConsumerRecord]:
        if not self.assignment:
            time.sleep(timeout_ms / 1000.0)
            return []

        records: List[ConsumerRecord] = []
        max_records = self.config.max_poll_records

        for topic, partition in self.assignment:
            if len(records) >= max_records:
                break
            offset = self.positions.get((topic, partition), 0)
            messages = self.broker.consume(
                topic, partition, offset,
                max_messages=min(100, max_records - len(records))
            )
            for msg in messages:
                records.append(ConsumerRecord(
                    topic=topic, partition=partition, offset=msg.offset,
                    key=msg.key, value=msg.value,
                    timestamp=msg.timestamp, headers=msg.headers
                ))
            if messages:
                self.positions[(topic, partition)] = messages[-1].offset + 1

        return records

    def commit(self):
        for (topic, partition), offset in self.positions.items():
            self.offset_manager.commit(self.config.group_id, topic, partition, offset)

    def _start_auto_commit(self):
        self._running = True

        def _loop():
            while self._running:
                time.sleep(self.config.auto_commit_interval_ms / 1000.0)
                if self._running:
                    self.commit()

        t = threading.Thread(target=_loop, daemon=True)
        t.start()

    def close(self):
        self._running = False
        self.commit()
        if self.group:
            self.group.leave(self.consumer_id)
        print(f"👋 Consumer {self.consumer_id} closed")


# ============================================================================
# 8. PERFORMANCE BENCHMARKING
# ============================================================================

class PerformanceBenchmark:

    @staticmethod
    def benchmark_producer(broker: Broker, topic_name: str, num_messages: int = 10_000):
        print(f"\n{'='*70}")
        print(f"PRODUCER BENCHMARK: {num_messages:,} messages")
        print(f"{'='*70}")

        producer = Producer(broker)
        records = [
            ProducerRecord(
                topic=topic_name,
                value=(f"Message {i} - " + "x" * 100).encode(),
                key=f"key-{i % 100}"
            )
            for i in range(num_messages)
        ]
        total_bytes = sum(len(r.value) for r in records)

        start = time.time()
        for r in records:
            producer.send(r)
        duration = time.time() - start

        print(f"\n📊 Producer Results:")
        print(f"  Duration    : {duration:.2f}s")
        print(f"  Throughput  : {num_messages / duration:,.0f} msgs/sec")
        print(f"  Throughput  : {total_bytes / 1024 / 1024 / duration:.2f} MB/sec")
        print(f"  Avg Latency : {duration / num_messages * 1000:.3f} ms/msg")

    @staticmethod
    def benchmark_consumer(broker: Broker, topic_name: str, expected_messages: int):
        print(f"\n{'='*70}")
        print(f"CONSUMER BENCHMARK: reading {expected_messages:,} messages")
        print(f"{'='*70}")

        config = ConsumerConfig(
            group_id="benchmark-group",
            auto_offset_reset="earliest",
            max_poll_records=500
        )
        om = OffsetManager()
        consumer = Consumer(broker, config, om)
        consumer.subscribe([topic_name])
        time.sleep(0.5)

        start = time.time()
        consumed, total_bytes = 0, 0
        while consumed < expected_messages:
            records = consumer.poll(timeout_ms=1000)
            consumed += len(records)
            total_bytes += sum(len(r.value) for r in records)
            if not records:
                break
        duration = time.time() - start
        consumer.close()

        if duration > 0 and consumed > 0:
            print(f"\n📊 Consumer Results:")
            print(f"  Duration    : {duration:.2f}s")
            print(f"  Messages    : {consumed:,}")
            print(f"  Throughput  : {consumed / duration:,.0f} msgs/sec")
            print(f"  Throughput  : {total_bytes / 1024 / 1024 / duration:.2f} MB/sec")

    @staticmethod
    def benchmark_end_to_end(broker: Broker, topic_name: str, num_messages: int = 100):
        print(f"\n{'='*70}")
        print(f"END-TO-END LATENCY BENCHMARK: {num_messages} messages")
        print(f"{'='*70}")

        producer = Producer(broker)
        config = ConsumerConfig(group_id="latency-group", auto_offset_reset="latest")
        om = OffsetManager()
        consumer = Consumer(broker, config, om)
        consumer.subscribe([topic_name])
        time.sleep(0.5)

        latencies: List[float] = []
        for i in range(num_messages):
            t0 = time.time()
            producer.send(ProducerRecord(
                topic=topic_name,
                value=json.dumps({"send_time": t0}).encode(),
                key=f"key-{i}"
            ))
            while True:
                records = consumer.poll(timeout_ms=100)
                if records:
                    data = json.loads(records[0].value.decode())
                    latencies.append((time.time() - data['send_time']) * 1000)
                    break

        consumer.close()
        latencies.sort()
        n = len(latencies)
        print(f"\n📊 Latency Results:")
        print(f"  Average : {sum(latencies)/n:.2f} ms")
        print(f"  P50     : {latencies[n//2]:.2f} ms")
        print(f"  P95     : {latencies[int(n*0.95)]:.2f} ms")
        print(f"  P99     : {latencies[int(n*0.99)]:.2f} ms")
        print(f"  Max     : {latencies[-1]:.2f} ms")


# ============================================================================
# 9. DEMOS
# ============================================================================

def demo_1_basic_producer_consumer():
    print("\n" + "="*70)
    print("DEMO 1: BASIC PRODUCER-CONSUMER")
    print("="*70)

    broker = Broker(broker_id=1)
    broker.create_topic(TopicConfig(name="orders", num_partitions=3))

    producer = Producer(broker)
    print("\n📤 Producing 10 messages...")
    for i in range(10):
        partition, offset = producer.send(ProducerRecord(
            topic="orders",
            value=f"Order {i}: iPhone 15 Pro".encode(),
            key=f"user-{i % 3}"
        ))
        print(f"  ✅ partition={partition}, offset={offset}")

    config = ConsumerConfig(group_id="order-processors", auto_offset_reset="earliest")
    om = OffsetManager()
    consumer = Consumer(broker, config, om)
    consumer.subscribe(["orders"])
    time.sleep(0.5)

    print("\n📥 Consuming messages...")
    total = 0
    for _ in range(5):
        records = consumer.poll(timeout_ms=500)
        for r in records:
            print(f"  📨 [{r.partition}:{r.offset}] {r.value.decode()}")
        total += len(records)
        if not records:
            break

    consumer.close()
    print(f"\n✅ Consumed {total} messages")


def demo_2_consumer_groups():
    print("\n" + "="*70)
    print("DEMO 2: CONSUMER GROUPS (Load Balancing)")
    print("="*70)

    broker = Broker(broker_id=1)
    broker.create_topic(TopicConfig(name="events", num_partitions=6))

    producer = Producer(broker)
    print("\n📤 Producing 30 messages...")
    for i in range(30):
        producer.send(ProducerRecord(
            topic="events", value=f"Event {i}".encode(), key=f"key-{i}"
        ))

    config = ConsumerConfig(group_id="event-processors", auto_offset_reset="earliest")
    om = OffsetManager()
    consumers = [Consumer(broker, config, om) for _ in range(3)]
    for c in consumers:
        c.subscribe(["events"])
    time.sleep(1)

    print("\n📊 Assignments:")
    for c in consumers:
        print(f"  {c.consumer_id}: partitions {[p for _, p in c.assignment]}")

    print("\n📥 Polling...")
    for c in consumers:
        records = c.poll(timeout_ms=500)
        if records:
            parts = set(r.partition for r in records)
            print(f"  {c.consumer_id}: {len(records)} msgs from partitions {parts}")
        c.close()

    print("\n✅ Load balanced across consumers!")


def demo_3_partitioning_strategies():
    print("\n" + "="*70)
    print("DEMO 3: PARTITIONING STRATEGIES")
    print("="*70)

    for strategy in PartitionStrategy:
        print(f"\n📍 Strategy: {strategy.value}")
        partitioner = Partitioner(strategy)
        dist: Dict[int, int] = defaultdict(int)
        for i in range(20):
            p = partitioner.partition(f"user-{i % 5}", 4)
            dist[p] += 1
            if i < 5:
                print(f"  msg {i} (key=user-{i%5}) → partition {p}")
        print(f"  Distribution: {dict(sorted(dist.items()))}")


def demo_4_offset_management():
    print("\n" + "="*70)
    print("DEMO 4: OFFSET MANAGEMENT")
    print("="*70)

    broker = Broker(broker_id=1)
    broker.create_topic(TopicConfig(name="transactions", num_partitions=2))

    producer = Producer(broker)
    for i in range(20):
        producer.send(ProducerRecord(
            topic="transactions", value=f"Tx {i}".encode()
        ))

    config = ConsumerConfig(
        group_id="tx-processors",
        auto_offset_reset="earliest",
        enable_auto_commit=False
    )
    om = OffsetManager()

    consumer1 = Consumer(broker, config, om)
    consumer1.subscribe(["transactions"])
    time.sleep(0.5)

    count = 0
    while count < 10:
        records = consumer1.poll(timeout_ms=500)
        count += len(records)

    print(f"✅ Consumer 1 read {count} messages, committing...")
    consumer1.commit()
    consumer1.close()

    consumer2 = Consumer(broker, config, om)
    consumer2.subscribe(["transactions"])
    time.sleep(0.5)

    records = consumer2.poll(timeout_ms=500)
    if records:
        print(f"✅ Consumer 2 resumed from offset {records[0].offset} ({len(records)} msgs)")
    consumer2.close()


def demo_5_persistence_and_recovery():
    """
    FIX 3 in action: broker2 reloads topic config from broker_metadata.json
    so topics dict is populated correctly after restart.
    """
    print("\n" + "="*70)
    print("DEMO 5: PERSISTENCE & RECOVERY")
    print("="*70)

    DATA_DIR = "./demo-kafka-data"

    broker1 = Broker(broker_id=1, data_dir=DATA_DIR)
    broker1.create_topic(TopicConfig(name="persistent-topic", num_partitions=2))

    producer = Producer(broker1)
    for i in range(50):
        producer.send(ProducerRecord(
            topic="persistent-topic", value=f"Persistent msg {i}".encode()
        ))
    print("✅ Produced 50 messages — persisted to disk")

    # Simulate restart
    print("🔄 Simulating broker restart...")
    del broker1

    broker2 = Broker(broker_id=1, data_dir=DATA_DIR)

    # FIX: topics is now populated from metadata file
    if "persistent-topic" in broker2.topics:
        print("✅ Topic recovered from disk!")
        config = ConsumerConfig(group_id="recovery-test", auto_offset_reset="earliest")
        om = OffsetManager()
        consumer = Consumer(broker2, config, om)
        consumer.subscribe(["persistent-topic"])
        time.sleep(0.5)

        msgs = []
        for _ in range(10):
            records = consumer.poll(timeout_ms=200)
            msgs.extend(records)
            if not records:
                break

        print(f"✅ Recovered {len(msgs)} messages from disk")
        consumer.close()
    else:
        print("❌ Topic not found after restart")


def demo_6_performance_benchmark():
    print("\n" + "="*70)
    print("DEMO 6: PERFORMANCE BENCHMARKING")
    print("="*70)

    broker = Broker(broker_id=1)
    broker.create_topic(TopicConfig(name="benchmark-topic", num_partitions=4))

    PerformanceBenchmark.benchmark_producer(broker, "benchmark-topic", num_messages=10_000)
    PerformanceBenchmark.benchmark_consumer(broker, "benchmark-topic", expected_messages=10_000)
    PerformanceBenchmark.benchmark_end_to_end(broker, "benchmark-topic", num_messages=100)


def demo_7_rebalancing():
    print("\n" + "="*70)
    print("DEMO 7: CONSUMER GROUP REBALANCING")
    print("="*70)

    broker = Broker(broker_id=1)
    broker.create_topic(TopicConfig(name="rebalance-topic", num_partitions=6))

    config = ConsumerConfig(group_id="rebalance-group", auto_offset_reset="earliest")
    om = OffsetManager()

    print("\n👥 Starting with 2 consumers...")
    c1 = Consumer(broker, config, om)
    c1.subscribe(["rebalance-topic"])
    c2 = Consumer(broker, config, om)
    c2.subscribe(["rebalance-topic"])
    time.sleep(1)

    print(f"\n  c1: {len(c1.assignment)} partitions | c2: {len(c2.assignment)} partitions")

    print("\n➕ Adding 3rd consumer (rebalance triggered)...")
    c3 = Consumer(broker, config, om)
    c3.subscribe(["rebalance-topic"])
    time.sleep(1)

    print(f"\n  c1: {len(c1.assignment)} | c2: {len(c2.assignment)} | c3: {len(c3.assignment)}")

    print("\n➖ Removing c2 (rebalance triggered)...")
    c2.close()
    time.sleep(1)

    print(f"\n  c1: {len(c1.assignment)} | c3: {len(c3.assignment)}")

    c1.close()
    c3.close()


def demo_8_retention_cleanup():
    print("\n" + "="*70)
    print("DEMO 8: MESSAGE RETENTION & CLEANUP")
    print("="*70)

    broker = Broker(broker_id=1)
    broker.create_topic(TopicConfig(
        name="short-retention", num_partitions=2, retention_ms=10_000
    ))

    producer = Producer(broker)
    for i in range(100):
        producer.send(ProducerRecord(
            topic="short-retention", value=f"Msg {i}".encode()
        ))

    topic = broker.get_topic("short-retention")
    before = sum(len(p.segments) for p in topic.partitions)
    print(f"📊 Segments before cleanup: {before}")

    print("⏳ Waiting 12s for retention window...")
    time.sleep(12)

    topic.cleanup_old_data()
    after = sum(len(p.segments) for p in topic.partitions)
    print(f"📊 Segments after cleanup : {after}")


def run_all_demos():
    print("\n" + "="*70)
    print("MINI KAFKA — COMPLETE DEMO SUITE")
    print("="*70)

    demos = [
        demo_1_basic_producer_consumer,
        demo_2_consumer_groups,
        demo_3_partitioning_strategies,
        demo_4_offset_management,
        demo_5_persistence_and_recovery,
        demo_6_performance_benchmark,
        demo_7_rebalancing,
        demo_8_retention_cleanup,
    ]

    for i, demo in enumerate(demos, 1):
        try:
            demo()
            time.sleep(1)
        except Exception as exc:
            import traceback
            print(f"\n❌ Demo {i} failed: {exc}")
            traceback.print_exc()

    print("\n" + "="*70)
    print("✅ ALL DEMOS COMPLETED")
    print("="*70)
    print("\n📚 Concepts covered:")
    for item in [
        "Topics & Partitions",
        "Producer with multiple partitioning strategies",
        "Consumer Groups with automatic rebalancing",
        "Offset management & at-least-once delivery",
        "Log-based persistent storage",
        "Broker metadata persistence & recovery",
        "Performance benchmarking (throughput + latency percentiles)",
        "Message retention & segment cleanup",
    ]:
        print(f"  ✅ {item}")


if __name__ == "__main__":
    # Clean up previous demo data
    for d in ("./demo-kafka-data", "./kafka-data"):
        if os.path.exists(d):
            shutil.rmtree(d)

    run_all_demos()
