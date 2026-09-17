import os
import sys
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from broker import Broker, Consumer, ConsumerGroup, Producer


class TestPartitioning(unittest.TestCase):
    def test_same_key_lands_in_same_partition(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=6)
        producer = Producer(broker, "t")

        msgs = [producer.send(value=f"v{i}", key="user-42") for i in range(10)]
        partitions = {m.partition for m in msgs}

        self.assertEqual(len(partitions), 1, "all messages with the same key must land in one partition")

    def test_ordering_preserved_within_partition(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=3)
        producer = Producer(broker, "t")

        sent = [producer.send(value=f"v{i}", key="same-key") for i in range(20)]
        partition_index = sent[0].partition
        topic = broker.get_topic("t")
        log = topic.partitions[partition_index].read_from(0, 1000)

        offsets = [m.offset for m in log]
        values = [m.value for m in log]
        self.assertEqual(offsets, sorted(offsets), "offsets within a partition must be monotonically increasing")
        self.assertEqual(values, [f"v{i}" for i in range(20)], "values must be read back in publish order")

    def test_round_robin_when_no_key(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=4)
        producer = Producer(broker, "t")

        msgs = [producer.send(value=i, key=None) for i in range(8)]
        partitions_hit = [m.partition for m in msgs]

        self.assertEqual(partitions_hit, [0, 1, 2, 3, 0, 1, 2, 3])


class TestConsumerGroupAssignment(unittest.TestCase):
    def test_partition_assignment_has_no_overlap_and_full_coverage(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=5)
        group = ConsumerGroup(broker, "t", group_id="g1")
        Consumer(broker, "t", group, consumer_id="c1")
        Consumer(broker, "t", group, consumer_id="c2")
        Consumer(broker, "t", group, consumer_id="c3")

        assignment = group.assignment()
        all_assigned = []
        for parts in assignment.values():
            all_assigned.extend(parts)

        self.assertEqual(sorted(all_assigned), list(range(5)), "every partition must be assigned exactly once")
        seen = set()
        for parts in assignment.values():
            for p in parts:
                self.assertNotIn(p, seen, "no partition should be assigned to more than one consumer")
                seen.add(p)

    def test_two_consumers_split_multi_partition_topic(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=4)
        group = ConsumerGroup(broker, "t", group_id="g1")
        c1 = Consumer(broker, "t", group, consumer_id="c1")
        c2 = Consumer(broker, "t", group, consumer_id="c2")

        p1 = set(c1.assigned_partitions())
        p2 = set(c2.assigned_partitions())

        self.assertTrue(p1, "consumer 1 should own at least one partition")
        self.assertTrue(p2, "consumer 2 should own at least one partition")
        self.assertEqual(p1 & p2, set(), "consumers in the same group must not share partitions")
        self.assertEqual(p1 | p2, {0, 1, 2, 3})


class TestOffsetsAndDelivery(unittest.TestCase):
    def test_offset_commit_and_resume_after_restart(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=1)
        producer = Producer(broker, "t")
        for i in range(5):
            producer.send(value=f"v{i}", key="k")

        group = ConsumerGroup(broker, "t", group_id="g1")
        consumer = Consumer(broker, "t", group, consumer_id="c1")

        first_batch = consumer.poll(max_messages=3)
        self.assertEqual([m.value for m in first_batch], ["v0", "v1", "v2"])
        consumer.commit()

        # Simulate a restart: a brand-new Consumer object for the same
        # group/id must resume from the committed offset, not from 0.
        restarted = Consumer(broker, "t", group, consumer_id="c1")
        second_batch = restarted.poll(max_messages=10)
        self.assertEqual([m.value for m in second_batch], ["v3", "v4"])

    def test_uncommitted_messages_are_redelivered_on_restart(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=1)
        producer = Producer(broker, "t")
        for i in range(3):
            producer.send(value=f"v{i}", key="k")

        group = ConsumerGroup(broker, "t", group_id="g1")
        consumer = Consumer(broker, "t", group, consumer_id="c1")

        batch = consumer.poll(max_messages=10)
        self.assertEqual(len(batch), 3)
        # NOTE: no consumer.commit() here -- simulate a crash before commit.

        restarted = Consumer(broker, "t", group, consumer_id="c1")
        redelivered = restarted.poll(max_messages=10)

        self.assertEqual(
            [m.value for m in redelivered], ["v0", "v1", "v2"],
            "uncommitted messages must be redelivered after a restart",
        )

    def test_multiple_consumer_groups_read_independently(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=1)
        producer = Producer(broker, "t")
        for i in range(4):
            producer.send(value=f"v{i}", key="k")

        group_a = ConsumerGroup(broker, "t", group_id="group-a")
        group_b = ConsumerGroup(broker, "t", group_id="group-b")
        consumer_a = Consumer(broker, "t", group_a, consumer_id="c1")
        consumer_b = Consumer(broker, "t", group_b, consumer_id="c1")

        batch_a = consumer_a.poll(max_messages=2)
        consumer_a.commit()
        self.assertEqual([m.value for m in batch_a], ["v0", "v1"])

        # group-b has committed nothing yet, so it should still see
        # everything from the start, independent of group-a's progress.
        batch_b = consumer_b.poll(max_messages=10)
        self.assertEqual([m.value for m in batch_b], ["v0", "v1", "v2", "v3"])
        consumer_b.commit()

        # group-a resumes after its own committed offset (2), unaffected
        # by group-b having read further ahead.
        rest_of_a = consumer_a.poll(max_messages=10)
        self.assertEqual([m.value for m in rest_of_a], ["v2", "v3"])

    def test_publishing_with_zero_consumers_does_not_lose_messages(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=2)
        producer = Producer(broker, "t")
        for i in range(6):
            producer.send(value=f"v{i}", key=f"key-{i}")

        # No consumer group / consumer has been created yet.
        topic = broker.get_topic("t")
        total_logged = sum(len(p) for p in topic.partitions)
        self.assertEqual(total_logged, 6)

        # A consumer that joins later still sees every message from the
        # beginning (offset 0), since nothing has been committed.
        group = ConsumerGroup(broker, "t", group_id="late-group")
        consumer = Consumer(broker, "t", group, consumer_id="c1")
        received = consumer.poll(max_messages=100)
        self.assertEqual(len(received), 6)

    def test_commit_only_advances_owned_partitions(self):
        broker = Broker()
        broker.create_topic("t", num_partitions=2)
        producer = Producer(broker, "t")
        # Force messages onto both partitions deterministically via keys
        # that we know hash to different partitions is not guaranteed,
        # so instead publish keyless (round robin) to hit both.
        for i in range(4):
            producer.send(value=f"v{i}", key=None)

        group = ConsumerGroup(broker, "t", group_id="g1")
        consumer = Consumer(broker, "t", group, consumer_id="only-consumer")

        self.assertEqual(set(consumer.assigned_partitions()), {0, 1})
        consumer.poll(max_messages=100)
        consumer.commit()

        offsets = consumer.committed_offsets()
        self.assertEqual(offsets.get(0), 2)
        self.assertEqual(offsets.get(1), 2)


if __name__ == "__main__":
    unittest.main()
