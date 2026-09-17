#!/usr/bin/env python3
"""
Demo: multiple producers writing to a multi-partition topic, consumed by
a consumer group with two consumers that split the partitions between
them.

Run:
    python3 demo.py
"""

from broker import Broker, Consumer, ConsumerGroup, Producer


def main() -> None:
    broker = Broker()
    broker.create_topic("orders", num_partitions=4)

    # --- Producers -----------------------------------------------------
    producer_a = Producer(broker, "orders")
    producer_b = Producer(broker, "orders")

    customers = ["alice", "bob", "carol", "dave"]
    for i in range(12):
        customer = customers[i % len(customers)]
        producer = producer_a if i % 2 == 0 else producer_b
        msg = producer.send(value=f"order-{i}", key=customer)
        print(f"[produced] key={customer:<6} value=order-{i:<2} -> partition {msg.partition}, offset {msg.offset}")

    print()

    # --- Consumer group with 2 consumers --------------------------------
    group = ConsumerGroup(broker, "orders", group_id="order-processors")
    consumer_1 = Consumer(broker, "orders", group, consumer_id="consumer-1")
    consumer_2 = Consumer(broker, "orders", group, consumer_id="consumer-2")

    print(f"consumer-1 assigned partitions: {consumer_1.assigned_partitions()}")
    print(f"consumer-2 assigned partitions: {consumer_2.assigned_partitions()}")
    print()

    for consumer in (consumer_1, consumer_2):
        batch = consumer.poll(max_messages=10)
        for msg in batch:
            print(f"[{consumer.consumer_id}] processing partition={msg.partition} offset={msg.offset} key={msg.key} value={msg.value}")
        consumer.commit()
        print(f"[{consumer.consumer_id}] committed offsets: {consumer.committed_offsets()}")
        print()

    # --- Simulate a consumer restart: nothing left uncommitted, so a
    # fresh poll finds no messages to redeliver. -------------------------
    restarted_consumer_1 = Consumer(broker, "orders", group, consumer_id="consumer-1")
    remaining = restarted_consumer_1.poll(max_messages=10)
    print(f"[consumer-1 restarted] messages waiting after clean commit: {len(remaining)}")

    # Publish a few more messages, poll but DON'T commit, then simulate a
    # crash/restart to show at-least-once redelivery.
    for i in range(12, 15):
        customer = customers[i % len(customers)]
        producer_a.send(value=f"order-{i}", key=customer)

    uncommitted_read = restarted_consumer_1.poll(max_messages=10)
    print(f"[consumer-1] read {len(uncommitted_read)} new messages but will NOT commit (simulating a crash)")

    crashed_and_restarted = Consumer(broker, "orders", group, consumer_id="consumer-1")
    redelivered = crashed_and_restarted.poll(max_messages=10)
    print(f"[consumer-1 restarted again] redelivered {len(redelivered)} uncommitted messages: "
          f"{[(m.partition, m.offset) for m in redelivered]}")


if __name__ == "__main__":
    main()
