# mini-message-broker

A small, dependency-free, in-process pub/sub message broker in pure Python —
a tiny "Kafka-lite". No Redis, no Kafka, no RabbitMQ: everything lives in a
few hundred lines of stdlib Python, built to demonstrate the core ideas
behind a partitioned log broker with consumer groups.

## Features

- **Topics with partitions**: a topic is split into `N` partitions, each an
  append-only log.
- **Key-based routing**: producers publish `(key, value)` pairs; the
  partition is chosen by hashing the key (round-robin if no key is given),
  so messages for the same key always land in the same partition and keep
  their relative order.
- **Consumer groups**: multiple consumers in the same group split a topic's
  partitions between them — no two consumers in the same group ever read
  the same partition. Each consumer tracks its own read offset per
  partition and resumes where it left off.
- **At-least-once delivery**: a consumer must explicitly call `commit()`
  after processing a batch. If it crashes/restarts before committing, the
  next poll redelivers the uncommitted messages.
- **Independent consumer groups**: separate groups reading the same topic
  track completely separate offsets.

## Architecture

```
Producer ---> Topic ---> [Partition 0 log]
                     ---> [Partition 1 log]
                     ---> [Partition N log]

ConsumerGroup "g1"
  ├── Consumer "c1" -> owns partitions {0, 2}
  └── Consumer "c2" -> owns partitions {1, 3}

Broker keeps: committed_offset[(topic, group, partition)]
```

### Storage

Each `Partition` is an append-only Python list of `Message` records
(`offset`, `key`, `value`, `partition`) guarded by a lock. This is the
simplest possible append-only log. The public API (`append()` /
`read_from(offset, max_messages)`) is intentionally the same shape a
disk-backed log would use (e.g. one file per partition, offsets as byte
or line positions), so swapping in a file-backed `Partition` later would
not require changing `Topic`, `Broker`, `ConsumerGroup`, or `Consumer`.
In-memory was chosen for this project to keep the demo and tests fast and
dependency-free.

### Partitioning

- If a message has a `key`, the partition is
  `stable_hash(key) % num_partitions`, using an MD5-based hash (Python's
  built-in `hash()` is randomized per process for strings, which would make
  partitioning non-deterministic across runs — MD5 keeps it stable).
- If a message has no key (`key=None`), partitions are chosen round-robin.
- Because a key always maps to the same partition, and a partition is a
  single append-only log, all messages sharing a key are strictly ordered
  relative to each other.

### Consumer groups & partition assignment

`ConsumerGroup.assignment()` sorts the group's member consumer ids and the
topic's partition indices, then assigns `partition i` to
`sorted(members)[i % len(members)]`. This guarantees:

- every partition is assigned to exactly one consumer in the group,
- no partition is ever assigned to two consumers in the same group,
- the assignment is deterministic given the current membership (a
  "rebalance" just recomputes it).

### Offsets & delivery semantics

For every `(topic, group, partition)` triple the `Broker` stores a durable
**committed offset** — the next offset that group should read from that
partition. A `Consumer`:

1. On creation, seeds an **in-memory read cursor** per owned partition from
   the broker's committed offset.
2. `poll()` reads from that cursor and advances it — but this is *not*
   persisted yet.
3. `commit()` writes the current cursor back to the broker as the new
   committed offset.

If a consumer polls messages and then crashes (or is simply never
`commit()`-ed) before committing, a fresh `Consumer` object created for
the same `(group, consumer_id)` re-seeds its cursor from the *last
committed* offset — so the unacknowledged messages are read again. This is
at-least-once delivery: messages are never lost, but may be redelivered if
a consumer dies mid-batch.

Because offsets are tracked per `(topic, group, partition)`, two different
consumer groups reading the same topic never interfere with each other's
progress, and publishing to a topic that currently has zero consumers
never drops messages — they simply sit in the partition log until some
consumer group reads them (a new group starts from offset 0, i.e. the
beginning of the log).

## Running the demo

```bash
python3 demo.py
```

The demo:

1. Creates a topic `orders` with 4 partitions.
2. Uses two producers to publish 12 keyed messages (keyed by customer name).
3. Starts a consumer group `order-processors` with two consumers, which
   split the 4 partitions between them (2 each).
4. Has each consumer poll, print, and commit its batch.
5. Simulates a consumer restart with nothing outstanding (no redelivery).
6. Publishes a few more messages, has a consumer poll them **without**
   committing, then simulates a crash + restart to show those uncommitted
   messages get redelivered.

### Example output

```
[produced] key=alice  value=order-0  -> partition 1, offset 0
[produced] key=bob    value=order-1  -> partition 3, offset 0
[produced] key=carol  value=order-2  -> partition 0, offset 0
...

consumer-1 assigned partitions: [0, 2]
consumer-2 assigned partitions: [1, 3]

[consumer-1] processing partition=0 offset=0 key=carol value=order-2
[consumer-1] processing partition=0 offset=1 key=carol value=order-6
...
[consumer-1] committed offsets: {0: 3, 2: 3}

[consumer-2] processing partition=1 offset=0 key=alice value=order-0
...
[consumer-2] committed offsets: {1: 3, 3: 3}

[consumer-1 restarted] messages waiting after clean commit: 0
[consumer-1] read 2 new messages but will NOT commit (simulating a crash)
[consumer-1 restarted again] redelivered 2 uncommitted messages: [(0, 3), (2, 3)]
```

(Exact partition numbers depend on the hash of each key; run it yourself
to see the real assignment.)

## Running the tests

```bash
python3 -m unittest discover -s tests -v
```

No third-party packages are required — everything uses only the Python
standard library (`unittest`, `hashlib`, `threading`, `dataclasses`).

The test suite (`tests/test_broker.py`) covers:

- messages with the same key land in the same partition,
- ordering is preserved within a partition,
- round-robin partitioning for keyless messages,
- consumer group partition assignment has no overlap and full coverage,
- two consumers in a group split a multi-partition topic,
- offset commit + resume works across a simulated consumer restart,
- uncommitted messages are redelivered after a simulated restart,
- multiple consumer groups read the same topic independently,
- publishing to a topic with zero consumers does not lose messages,
- commit only advances the offsets of partitions a consumer actually owns.

CI (`.github/workflows/tests.yml`) runs this suite on every push and pull
request against Python 3.11 and 3.12.

## Project layout

```
mini-message-broker/
├── broker/
│   ├── __init__.py     # public API re-exports
│   └── broker.py       # Message, Partition, Topic, Broker, Producer,
│                        # ConsumerGroup, Consumer
├── demo.py              # runnable multi-producer / consumer-group demo
├── tests/
│   └── test_broker.py   # unit tests
└── .github/workflows/tests.yml
```

## License

MIT — see [LICENSE](LICENSE).
