"""Splits the training rows of a Job into partitions and spreads them.

Only the training split is partitioned: the test split stays with the
Coordinator for the final evaluation. Every partition gets at least one row,
and the first partitions absorb the remainder, so sizes differ by at most one.

Following the design, a Job has more partitions than Workers (by default
three per Worker, 9 for 3 Workers). Small partitions let a Worker run several
of them, let the more capable Workers take more, and let the Tasks of a failed
Worker be spread across the survivors instead of moving one large block.
"""

from collections import Counter
from collections.abc import Hashable, Mapping, Sequence
from typing import Any, TypeVar

# Internal Coordinator setting; the design asks to tune it in tests.
PARTITIONS_PER_WORKER = 3

_TaskId = TypeVar("_TaskId", bound=Hashable)
_NodeId = TypeVar("_NodeId", bound=Hashable)


def partition_count(
    train_rows: int,
    worker_count: int,
    partitions_per_worker: int = PARTITIONS_PER_WORKER,
) -> int:
    """Number of partitions for a Job, never more than its training rows."""
    if worker_count < 1 or partitions_per_worker < 1:
        raise ValueError("worker_count and partitions_per_worker must be positive.")
    return max(1, min(train_rows, worker_count * partitions_per_worker))


def distribute_tasks(
    task_ids: Sequence[_TaskId],
    node_ids_by_priority: Sequence[_NodeId],
    current_task_counts: Mapping[_NodeId, int] | None = None,
) -> list[tuple[_TaskId, _NodeId]]:
    """Assigns each Task to the Worker that currently has the fewest Tasks.

    Ties go to the Worker listed first, so ``node_ids_by_priority`` should be
    ordered by preference (lower load, more CPU and RAM). With 9 Tasks and 3
    Workers each gets 3; with 10, the preferred Worker gets the extra one.
    """
    if not node_ids_by_priority:
        return []
    counts = Counter({node: 0 for node in node_ids_by_priority})
    counts.update(
        {
            node: count
            for node, count in (current_task_counts or {}).items()
            if node in counts
        }
    )
    priority = {node: index for index, node in enumerate(node_ids_by_priority)}

    plan: list[tuple[_TaskId, _NodeId]] = []
    for task_id in task_ids:
        node = min(node_ids_by_priority, key=lambda n: (counts[n], priority[n]))
        plan.append((task_id, node))
        counts[node] += 1
    return plan


def build_partitions(train_rows: int, partition_count: int) -> list[dict[str, int]]:
    """Returns ``[{"partition_index": i, "rows_assigned": n}, ...]``."""
    if isinstance(train_rows, bool) or not isinstance(train_rows, int) or train_rows < 1:
        raise ValueError("train_rows must be a positive integer.")
    if isinstance(partition_count, bool) or not isinstance(partition_count, int):
        raise ValueError("partition_count must be an integer.")
    if not 1 <= partition_count <= train_rows:
        raise ValueError("partition_count must be between 1 and the number of training rows.")

    base_rows, extra_rows = divmod(train_rows, partition_count)
    return [
        {"partition_index": index, "rows_assigned": base_rows + (1 if index < extra_rows else 0)}
        for index in range(partition_count)
    ]


def partition_bounds(tasks: Sequence[Mapping[str, Any]]) -> dict[int, tuple[int, int]]:
    """Maps each partition_index to its ``[start, end)`` range of training rows.

    The ranges come from the Tasks' ``rows_assigned`` in partition order, so
    the Coordinator and every Worker derive the same bounds from the database.
    """
    bounds: dict[int, tuple[int, int]] = {}
    start = 0
    for task in sorted(tasks, key=lambda task: task["partition_index"]):
        rows = task.get("rows_assigned")
        if isinstance(rows, bool) or not isinstance(rows, int) or rows < 1:
            raise ValueError(
                f"Task for partition {task['partition_index']} has no valid rows_assigned."
            )
        bounds[task["partition_index"]] = (start, start + rows)
        start += rows
    return bounds
