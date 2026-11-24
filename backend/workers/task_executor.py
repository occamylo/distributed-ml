"""Executes gradient requests for the Tasks assigned to this Worker.

The Coordinator sends the current global weights for one Task. Before
computing anything the Worker checks, against CockroachDB, that:

* the Task is assigned to this Worker with the same ``attempt_id``;
* the request carries the leadership epoch that is currently valid.

A request that fails either check comes from a replaced attempt or from a
former Coordinator, and is rejected with 409. The Worker then computes the
mean gradient of its own partition of the training split.
"""

import logging
import threading
from dataclasses import dataclass
from typing import Any
from uuid import UUID

from coordinator.job_data import PreparedDatasetCache
from coordinator.partitioner import partition_bounds
from coordinator.preprocessing import DatasetPreparationError
from database.repository import (
    DEFAULT_CLUSTER_ID,
    get_job,
    get_leadership,
    get_task,
    list_tasks_by_job,
)
from workers.training_engine import ModelParameters, compute_gradient

EXECUTABLE_TASK_STATUSES = frozenset({"assigned", "running"})
logger = logging.getLogger(__name__)


class TaskRequestError(Exception):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(detail)
        self.status_code = status_code
        self.detail = detail


@dataclass(frozen=True)
class GradientRequest:
    task_id: UUID
    attempt_id: UUID
    leader_epoch: int
    training_epoch: int
    parameters: ModelParameters

    @classmethod
    def parse(cls, task_id: UUID, payload: Any) -> "GradientRequest":
        if not isinstance(payload, dict):
            raise TaskRequestError(400, "The request body must be a JSON object.")
        try:
            leader_epoch = payload["leader_epoch"]
            training_epoch = payload["training_epoch"]
            if any(
                isinstance(value, bool) or not isinstance(value, int) or value < 0
                for value in (leader_epoch, training_epoch)
            ):
                raise ValueError("Epochs must be non-negative integers.")
            return cls(
                task_id=task_id,
                attempt_id=UUID(str(payload["attempt_id"])),
                leader_epoch=leader_epoch,
                training_epoch=training_epoch,
                parameters=ModelParameters.from_dict(payload["parameters"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise TaskRequestError(400, f"Invalid gradient request ({exc}).") from exc


class TaskExecutor:
    def __init__(self, node_id: UUID) -> None:
        self.node_id = node_id
        self._prepared_datasets = PreparedDatasetCache()
        self._active_lock = threading.Lock()
        self._active_tasks = 0

    @property
    def active_tasks(self) -> int:
        with self._active_lock:
            return self._active_tasks

    def compute_task_gradient(self, request: GradientRequest) -> dict[str, Any]:
        with self._active_lock:
            self._active_tasks += 1
        try:
            return self._compute(request)
        finally:
            with self._active_lock:
                self._active_tasks -= 1

    def _compute(self, request: GradientRequest) -> dict[str, Any]:
        task = get_task(request.task_id)
        if task is None:
            raise TaskRequestError(404, "Task not found.")
        self._check_assignment(task, request)
        self._check_leader_epoch(request.leader_epoch)

        job = get_job(task["job_id"])
        if job is None or job.get("status") != "running":
            raise TaskRequestError(409, "The Job of this Task is not running.")

        try:
            prepared = self._prepared_datasets.get(job)
        except DatasetPreparationError as exc:
            raise TaskRequestError(422, f"Dataset preparation failed: {exc}") from exc

        try:
            start, end = partition_bounds(list_tasks_by_job(job["job_id"]))[
                task["partition_index"]
            ]
        except (KeyError, ValueError) as exc:
            raise TaskRequestError(409, f"Partition bounds are unavailable ({exc}).") from exc
        if end > len(prepared.y_train):
            raise TaskRequestError(
                409, "The partition exceeds the training rows of the prepared dataset."
            )

        try:
            gradient = compute_gradient(
                request.parameters,
                prepared.x_train[start:end],
                prepared.y_train[start:end],
            )
        except ValueError as exc:
            raise TaskRequestError(422, f"Gradient computation failed ({exc}).") from exc

        logger.debug(
            "Worker %s computed epoch %s for Task %s (rows %s-%s).",
            self.node_id,
            request.training_epoch,
            request.task_id,
            start,
            end,
        )
        return {
            "task_id": str(request.task_id),
            "attempt_id": str(request.attempt_id),
            "node_id": str(self.node_id),
            "partition_index": task["partition_index"],
            "row_range": [start, end],
            "training_epoch": request.training_epoch,
            "gradient": gradient.to_dict(),
        }

    def _check_assignment(self, task: dict[str, Any], request: GradientRequest) -> None:
        if task.get("node_id") != self.node_id:
            raise TaskRequestError(409, "The Task is not assigned to this Worker.")
        if task.get("attempt_id") != request.attempt_id:
            raise TaskRequestError(409, "The attempt_id is not the current attempt.")
        if task.get("status") not in EXECUTABLE_TASK_STATUSES:
            raise TaskRequestError(409, f"The Task is {task.get('status')}.")

    @staticmethod
    def _check_leader_epoch(leader_epoch: int) -> None:
        leadership = get_leadership(DEFAULT_CLUSTER_ID)
        if (
            leadership is None
            or leadership.get("is_valid") is not True
            or leadership.get("epoch") != leader_epoch
        ):
            raise TaskRequestError(
                409, "The request does not come from the current Coordinator epoch."
            )
