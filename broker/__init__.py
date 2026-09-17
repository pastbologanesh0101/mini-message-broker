"""
mini-message-broker
====================

A small, pure-stdlib, in-process pub/sub message broker with:

- Topics split into N partitions (append-only logs).
- Deterministic key -> partition hashing (round-robin when no key).
- Consumer groups that split partitions across their consumers.
- Per-partition, per-group committed offsets with at-least-once delivery
  (a consumer must explicitly ``commit()`` after processing; unread /
  uncommitted messages are redelivered on restart).
"""

from .broker import (
    Broker,
    Consumer,
    ConsumerGroup,
    Message,
    Partition,
    Producer,
    Topic,
)

__all__ = [
    "Broker",
    "Consumer",
    "ConsumerGroup",
    "Message",
    "Partition",
    "Producer",
    "Topic",
]
