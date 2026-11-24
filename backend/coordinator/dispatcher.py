"""Sends one training round to the Workers and collects their gradients.

For every Task of the Job, the Coordinator posts the current global weights
to the Worker that owns it, in parallel. The model must never be updated
with an incomplete set of gradients, so ``dispatch_round`` returns the valid
gradients together with the failed Tasks: the caller keeps the valid ones and
retries or reassigns only the failed Tasks, with the same weights and epoch
(README, step 10).

Each failure says whether it is ``retryable``: an unreachable Worker, a
timeout or a 5xx answer may be solved by another Worker; a rejected request
(4xx, for example an attempt that is no longer current) or an invalid answer
is not.
"""

import json
import logging
from collections.abc import Mapping, Sequence
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen
from uuid import UUID

from workers.coordination import internal_base_url
from workers.training_engine import Gradient, ModelParameters

GRADIENT_REQUEST_TIMEOUT_SECONDS = 60.0
logger = logging.getLogger(__name__)
Record = dict[str, Any]


@dataclass(frozen=True)
class TaskGradient:
    task_id: UUID
    node_id: UUID
    partition_index: int
    row_range: tuple[int, int]
    gradient: Gradient


@dataclass(frozen=True)
class TaskFailure:
    task_id: UUID
    node_id: UUID | None
    reason: str
    retryable: bool


@dataclass(frozen=True)
class RoundResult:
    gradients: dict[UUID, TaskGradient]
    failures: list[TaskFailure]


class RoundFailedError(Exception):
    """At least one Task did not return a usable gradient."""

    def __init__(self, failures: Sequence[TaskFailure]) -> None:
        self.failures = list(failures)
        details = "; ".join(f"{f.task_id}: {f.reason}" for f in self.failures)
        super().__init__(f"{len(self.failures)} Task(s) failed: {details}")


def request_gradients(
    tasks: Sequence[Record],
    nodes_by_id: Mapping[UUID, Record],
    parameters: ModelParameters,
    leader_epoch: int,
    training_epoch: int,
    timeout_seconds: float = GRADIENT_REQUEST_TIMEOUT_SECONDS,
) -> list[TaskGradient]:
    """Returns one gradient per Task, ordered by partition_index.

    Raises ``RoundFailedError`` if any Task fails.
    """
    result = dispatch_round(
        tasks, nodes_by_id, parameters, leader_epoch, training_epoch, timeout_seconds
    )
    if result.failures:
        raise RoundFailedError(result.failures)
    return sorted(result.gradients.values(), key=lambda gradient: gradient.partition_index)


def dispatch_round(
    tasks: Sequence[Record],
    nodes_by_id: Mapping[UUID, Record],
    parameters: ModelParameters,
    leader_epoch: int,
    training_epoch: int,
    timeout_seconds: float = GRADIENT_REQUEST_TIMEOUT_SECONDS,
) -> RoundResult:
    """Sends the round to every Task and returns valid gradients and failures."""
    if not tasks:
        raise ValueError("A training round needs at least one Task.")

    body = {
        "leader_epoch": leader_epoch,
        "training_epoch": training_epoch,
        "parameters": parameters.to_dict(),
    }
    with ThreadPoolExecutor(max_workers=len(tasks)) as pool:
        outcomes = list(
            pool.map(
                lambda task: _request_one(task, nodes_by_id, body, timeout_seconds),
                tasks,
            )
        )

    return RoundResult(
        gradients={o.task_id: o for o in outcomes if isinstance(o, TaskGradient)},
        failures=[o for o in outcomes if isinstance(o, TaskFailure)],
    )


def _request_one(
    task: Record,
    nodes_by_id: Mapping[UUID, Record],
    body: dict[str, Any],
    timeout_seconds: float,
) -> TaskGradient | TaskFailure:
    task_id = task["task_id"]
    node_id = task.get("node_id")
    node = nodes_by_id.get(node_id) if isinstance(node_id, UUID) else None
    base_url = internal_base_url(node) if node is not None else None
    if base_url is None:
        return TaskFailure(task_id, node_id, "The Task has no reachable Worker.", True)

    request = Request(
        f"{base_url}/internal/tasks/{task_id}/gradient",
        data=json.dumps({**body, "attempt_id": str(task.get("attempt_id"))}).encode("utf-8"),
        headers={"Content-Type": "application/json", "Accept": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            payload = json.loads(response.read())
    except HTTPError as exc:
        return TaskFailure(
            task_id, node_id, f"HTTP {exc.code}: {_error_detail(exc)}", exc.code >= 500
        )
    except (URLError, OSError, ValueError) as exc:
        # Connection refused or reset, timeout, truncated answer: the Worker
        # may be down, so another Worker can take the Task.
        return TaskFailure(task_id, node_id, f"{type(exc).__name__}: {exc}", True)

    try:
        return _validated(payload, task, body["training_epoch"])
    except (KeyError, TypeError, ValueError) as exc:
        return TaskFailure(task_id, node_id, f"Invalid response ({exc}).", False)


def _validated(payload: Any, task: Record, training_epoch: int) -> TaskGradient:
    if payload["task_id"] != str(task["task_id"]):
        raise ValueError("task_id does not match.")
    if payload["attempt_id"] != str(task.get("attempt_id")):
        raise ValueError("attempt_id does not match the current attempt.")
    if payload["training_epoch"] != training_epoch:
        raise ValueError("training_epoch does not match.")
    if payload["partition_index"] != task["partition_index"]:
        raise ValueError("partition_index does not match.")

    start, end = payload["row_range"]
    gradient = Gradient.from_dict(payload["gradient"])
    if gradient.sample_count != end - start or gradient.sample_count != task.get("rows_assigned"):
        raise ValueError("sample_count does not match the partition size.")
    return TaskGradient(
        task_id=task["task_id"],
        node_id=UUID(payload["node_id"]),
        partition_index=task["partition_index"],
        row_range=(start, end),
        gradient=gradient,
    )


def _error_detail(exc: HTTPError) -> str:
    try:
        return str(json.loads(exc.read()).get("detail"))
    except (ValueError, AttributeError, OSError):
        return exc.reason
