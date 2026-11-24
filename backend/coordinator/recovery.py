"""Decides where the Tasks of a failed Worker go (README, step 10).

Pure planning, no database access, so the rules can be tested directly:

* A Task may run at most ``MAX_TASK_ATTEMPTS`` times: the first assignment
  plus 2 additional retries, the initial value suggested by the design.
* Failed Tasks go to the surviving Workers that already take part in the
  Job, to the one with the fewest Tasks first (the coordinator last, since
  it reserves resources for coordinating). Only if none of them is alive
  may another online Worker take them.
* Without attempts left or Workers alive, the Job cannot continue.
"""

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any
from uuid import UUID

from coordinator.partitioner import distribute_tasks

MAX_TASK_ATTEMPTS = 3
Record = dict[str, Any]


class RecoveryError(Exception):
    """The failed Tasks cannot be recovered; the Job must fail."""


def plan_reassignments(
    failed_tasks: Sequence[Mapping[str, Any]],
    job_tasks: Sequence[Mapping[str, Any]],
    alive_node_ids: Sequence[UUID],
    coordinator_node_id: UUID | None,
    max_attempts: int = MAX_TASK_ATTEMPTS,
) -> list[tuple[Mapping[str, Any], UUID]]:
    """Returns ``(task, new_node_id)`` for every failed Task.

    ``alive_node_ids`` are the Workers that are online with a fresh
    Heartbeat, ordered by the Scheduler's preference.
    """
    for task in failed_tasks:
        attempts = task.get("attempts") or 0
        if attempts >= max_attempts:
            raise RecoveryError(
                f"Partition {task['partition_index']} used its {max_attempts} attempts "
                f"(first run plus {max_attempts - 1} retries)."
            )

    failed_ids = {task["task_id"] for task in failed_tasks}
    failed_nodes = {task.get("node_id") for task in failed_tasks}
    alive = [node for node in alive_node_ids if node not in failed_nodes]
    participants = {task.get("node_id") for task in job_tasks}
    candidates = [node for node in alive if node in participants] or alive
    if not candidates:
        raise RecoveryError("No Workers are left alive to take the failed Tasks.")

    # The coordinator goes last (architecture doc, section 7).
    candidates.sort(key=lambda node: node == coordinator_node_id)
    current_counts = Counter(
        task.get("node_id") for task in job_tasks if task["task_id"] not in failed_ids
    )
    by_id = {task["task_id"]: task for task in failed_tasks}
    ordered_ids = [
        task["task_id"] for task in sorted(failed_tasks, key=lambda t: t["partition_index"])
    ]
    return [
        (by_id[task_id], node)
        for task_id, node in distribute_tasks(ordered_ids, candidates, current_counts)
    ]
