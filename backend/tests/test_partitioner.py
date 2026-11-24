import unittest
from collections import Counter

from coordinator.partitioner import (
    build_partitions,
    distribute_tasks,
    partition_bounds,
    partition_count,
)


class PartitionCountTests(unittest.TestCase):
    def test_three_partitions_per_worker_by_default(self) -> None:
        self.assertEqual(partition_count(train_rows=400, worker_count=3), 9)
        self.assertEqual(partition_count(train_rows=400, worker_count=1), 3)

    def test_never_more_partitions_than_rows(self) -> None:
        self.assertEqual(partition_count(train_rows=5, worker_count=3), 5)
        self.assertEqual(partition_count(train_rows=1, worker_count=3), 1)


class DistributeTasksTests(unittest.TestCase):
    def test_nine_tasks_for_three_workers_gives_three_each(self) -> None:
        plan = distribute_tasks(list(range(9)), ["a", "b", "c"])

        self.assertEqual(Counter(node for _, node in plan), {"a": 3, "b": 3, "c": 3})

    def test_extra_tasks_go_to_the_preferred_workers(self) -> None:
        plan = distribute_tasks(list(range(10)), ["fuerte", "medio", "debil"])

        self.assertEqual(
            Counter(node for _, node in plan), {"fuerte": 4, "medio": 3, "debil": 3}
        )

    def test_tasks_go_to_the_worker_with_fewest_tasks(self) -> None:
        # "a" already holds 3 Tasks (for example after a partial assignment).
        plan = distribute_tasks(["t1", "t2", "t3"], ["a", "b"], {"a": 3, "b": 1})

        self.assertEqual([node for _, node in plan], ["b", "b", "a"])

    def test_counts_of_workers_outside_the_job_are_ignored(self) -> None:
        plan = distribute_tasks(["t1"], ["a"], {"x": 5})

        self.assertEqual(plan, [("t1", "a")])

    def test_no_workers_means_no_assignments(self) -> None:
        self.assertEqual(distribute_tasks(["t1"], []), [])


class PartitionerTests(unittest.TestCase):
    def test_partitions_cover_every_row_and_differ_by_at_most_one(self) -> None:
        partitions = build_partitions(train_rows=400, partition_count=3)

        self.assertEqual([p["rows_assigned"] for p in partitions], [134, 133, 133])
        self.assertEqual([p["partition_index"] for p in partitions], [0, 1, 2])

    def test_single_partition_takes_all_rows(self) -> None:
        self.assertEqual(
            build_partitions(train_rows=7, partition_count=1),
            [{"partition_index": 0, "rows_assigned": 7}],
        )

    def test_rejects_more_partitions_than_rows(self) -> None:
        with self.assertRaises(ValueError):
            build_partitions(train_rows=2, partition_count=3)

    def test_rejects_invalid_sizes(self) -> None:
        for train_rows, count in [(0, 1), (10, 0), (True, 1), (10, 1.5)]:
            with self.subTest(train_rows=train_rows, count=count):
                with self.assertRaises(ValueError):
                    build_partitions(train_rows, count)

    def test_bounds_are_contiguous_in_partition_order(self) -> None:
        tasks = [
            {"partition_index": 2, "rows_assigned": 133},
            {"partition_index": 0, "rows_assigned": 134},
            {"partition_index": 1, "rows_assigned": 133},
        ]

        self.assertEqual(
            partition_bounds(tasks),
            {0: (0, 134), 1: (134, 267), 2: (267, 400)},
        )

    def test_bounds_reject_tasks_without_rows(self) -> None:
        with self.assertRaises(ValueError):
            partition_bounds([{"partition_index": 0, "rows_assigned": None}])


if __name__ == "__main__":
    unittest.main()
