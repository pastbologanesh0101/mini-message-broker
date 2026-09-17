"""
Core broker implementation.

Design notes (see README.md for the full write-up):

* Storage: each partition is an append-only, in-memory list of Message
  records. This keeps the reference implementation small and dependency
  free; swapping ``Partition`` for something that mmaps / appends to a
  file on disk would not change any of the public API below, since all
  access already goes through ``append()`` and ``read_from()``.

* Partitioning: a message with a key is routed to
  ``stable_hash(key) % num_partitions``, so every message for the same
  key always lands in the same partition and therefore keeps its
  ordering relative to other messages with that key. Keyless messages
  are spread round-robin across partitions.

* Consumer groups: partitions of a topic are assigned to the consumers
  of a group with no overlap (a simple sorted round-robin assignment,
  recomputed whenever a consumer joins/leaves -- this is a "rebalance").

* Delivery semantics: for each ``(topic, group, partition)`` triple the
  broker keeps a durable *committed offset* (the next offset to be
  delivered). A consumer's ``poll()`` reads from an in-memory cursor
  that starts at the committed offset; ``commit()`` persists that
  cursor back to the broker. If a consumer never commits and "restarts"
  (a fresh ``Consumer`` object is created for the same group/id), its
  cursor is re-initialised from the last committed offset, so anything
  read-but-not-committed is redelivered -- i.e. at-least-once delivery.
"""

from __future__ import annotations

import hashlib
import threading
from dataclasses import dataclass, field
from typing import Dict, Hashable, List, Optional


def stable_hash(key: str) -> int:
    """A hash that is stable across processes/runs (unlike ``hash()``,
    which is salted per-process for strings unless PYTHONHASHSEED is
    fixed). Used to deterministically route keyed messages to a
    partition."""
    digest = hashlib.md5(key.encode("utf-8")).digest()
    return int.from_bytes(digest[:8], "big")


@dataclass(frozen=True)
class Message:
    offset: int
    key: Optional[str]
    value: object
    partition: int


class Partition:
    """An append-only log for a single partition of a topic."""

    def __init__(self, index: int):
        self.index = index
        self._log: List[Message] = []
        self._lock = threading.Lock()

    def append(self, key: Optional[str], value: object) -> Message:
        with self._lock:
            offset = len(self._log)
            msg = Message(offset=offset, key=key, value=value, partition=self.index)
            self._log.append(msg)
            return msg

    def read_from(self, offset: int, max_messages: int) -> List[Message]:
        with self._lock:
            return list(self._log[offset : offset + max_messages])

    def __len__(self) -> int:
        return len(self._log)


class Topic:
    """A topic made up of a fixed number of partitions."""

    def __init__(self, name: str, num_partitions: int):
        if num_partitions < 1:
            raise ValueError("num_partitions must be >= 1")
        self.name = name
        self.num_partitions = num_partitions
        self.partitions: List[Partition] = [Partition(i) for i in range(num_partitions)]
        self._round_robin_counter = 0
        self._rr_lock = threading.Lock()

    def partition_for_key(self, key: Optional[Hashable]) -> int:
        if key is None:
            with self._rr_lock:
                idx = self._round_robin_counter % self.num_partitions
                self._round_robin_counter += 1
                return idx
        return stable_hash(str(key)) % self.num_partitions

    def publish(self, key: Optional[str], value: object) -> Message:
        partition_index = self.partition_for_key(key)
        return self.partitions[partition_index].append(key, value)

    def __repr__(self) -> str:
        return f"Topic(name={self.name!r}, num_partitions={self.num_partitions})"


class Broker:
    """The broker owns all topics and all committed consumer-group
    offsets. Everything is kept in memory (dicts of dicts); this is the
    one place that would need to change to add persistence."""

    def __init__(self):
        self._topics: Dict[str, Topic] = {}
        # (topic, group) -> {partition_index: committed_offset}
        self._committed_offsets: Dict[tuple, Dict[int, int]] = {}
        self._lock = threading.Lock()

    def create_topic(self, name: str, num_partitions: int = 1) -> Topic:
        with self._lock:
            if name in self._topics:
                return self._topics[name]
            topic = Topic(name, num_partitions)
            self._topics[name] = topic
            return topic

    def get_topic(self, name: str) -> Topic:
        try:
            return self._topics[name]
        except KeyError:
            raise KeyError(f"No such topic: {name!r}") from None

    # ---- offset management -------------------------------------------------

    def get_committed_offset(self, topic: str, group: str, partition: int) -> int:
        return self._committed_offsets.get((topic, group), {}).get(partition, 0)

    def commit_offset(self, topic: str, group: str, partition: int, offset: int) -> None:
        with self._lock:
            self._committed_offsets.setdefault((topic, group), {})[partition] = offset

    def get_all_committed_offsets(self, topic: str, group: str) -> Dict[int, int]:
        return dict(self._committed_offsets.get((topic, group), {}))


class Producer:
    """Thin convenience wrapper around ``Topic.publish``."""

    def __init__(self, broker: Broker, topic: str):
        self._broker = broker
        self._topic_name = topic

    def send(self, value: object, key: Optional[str] = None) -> Message:
        topic = self._broker.get_topic(self._topic_name)
        return topic.publish(key, value)


class ConsumerGroup:
    """Tracks membership of a consumer group for one topic and computes
    a non-overlapping partition assignment for its members.

    Assignment strategy: sort partition indices and consumer ids, then
    hand out partitions round-robin (partition i goes to
    ``sorted(consumer_ids)[i % len(consumer_ids)]``). This guarantees:
      * every partition is assigned to exactly one consumer,
      * no two consumers in the group ever share a partition,
      * assignment is deterministic given the same membership.
    """

    def __init__(self, broker: Broker, topic: str, group_id: str):
        self.broker = broker
        self.topic_name = topic
        self.group_id = group_id
        self._member_ids: List[str] = []
        self._lock = threading.Lock()

    def join(self, consumer_id: str) -> None:
        with self._lock:
            if consumer_id not in self._member_ids:
                self._member_ids.append(consumer_id)

    def leave(self, consumer_id: str) -> None:
        with self._lock:
            if consumer_id in self._member_ids:
                self._member_ids.remove(consumer_id)

    def assignment(self) -> Dict[str, List[int]]:
        """Return {consumer_id: [partition indices]} for the current
        membership."""
        topic = self.broker.get_topic(self.topic_name)
        members = sorted(self._member_ids)
        result: Dict[str, List[int]] = {m: [] for m in members}
        if not members:
            return result
        for partition_index in range(topic.num_partitions):
            owner = members[partition_index % len(members)]
            result[owner].append(partition_index)
        return result

    def partitions_for(self, consumer_id: str) -> List[int]:
        return self.assignment().get(consumer_id, [])


class Consumer:
    """A single consumer that belongs to a ``ConsumerGroup``.

    ``poll()`` reads (but does not persist) the next batch of messages
    from each partition this consumer currently owns, advancing an
    in-memory cursor. Nothing is durably recorded as "processed" until
    ``commit()`` is called, which writes the current cursor back to the
    broker as the group's committed offset for that partition -- giving
    at-least-once delivery: if the process dies (or a new ``Consumer``
    is constructed for the same group/id) before ``commit()``, the next
    ``poll()`` starts again from the last committed offset and
    redelivers whatever was read-but-not-committed.
    """

    def __init__(self, broker: Broker, topic: str, group: ConsumerGroup, consumer_id: str):
        self.broker = broker
        self.topic_name = topic
        self.group = group
        self.consumer_id = consumer_id
        group.join(consumer_id)
        # in-memory read cursor per partition, seeded from committed offsets
        self._cursor: Dict[int, int] = {}
        self._sync_cursor_with_committed()

    def _sync_cursor_with_committed(self) -> None:
        for partition_index in self.group.partitions_for(self.consumer_id):
            committed = self.broker.get_committed_offset(
                self.topic_name, self.group.group_id, partition_index
            )
            # Only (re)seed partitions we don't already have a cursor for,
            # so a rebalance doesn't rewind partitions we still own.
            self._cursor.setdefault(partition_index, committed)

    def assigned_partitions(self) -> List[int]:
        self._sync_cursor_with_committed()
        return self.group.partitions_for(self.consumer_id)

    def poll(self, max_messages: int = 10) -> List[Message]:
        """Read up to ``max_messages`` per assigned partition. Advances
        this consumer's in-memory cursor but does NOT commit -- call
        ``commit()`` once the batch has been processed."""
        topic = self.broker.get_topic(self.topic_name)
        batch: List[Message] = []
        for partition_index in self.assigned_partitions():
            start = self._cursor.get(partition_index, 0)
            messages = topic.partitions[partition_index].read_from(start, max_messages)
            if messages:
                self._cursor[partition_index] = messages[-1].offset + 1
                batch.extend(messages)
        return batch

    def commit(self) -> None:
        """Persist this consumer's current read cursor as the group's
        committed offset, for every partition it owns."""
        for partition_index in self.assigned_partitions():
            offset = self._cursor.get(partition_index, 0)
            self.broker.commit_offset(
                self.topic_name, self.group.group_id, partition_index, offset
            )

    def committed_offsets(self) -> Dict[int, int]:
        return self.broker.get_all_committed_offsets(self.topic_name, self.group.group_id)
