import logging
import math
import threading
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from psycopg import Error as PsycopgError
from psycopg_pool import PoolTimeout

from database.repository import (
    DEFAULT_CLUSTER_ID,
    assign_task,
    create_tasks_for_job,
    get_dataset,
    get_leadership,
    list_jobs,
    list_nodes,
    list_tasks_by_job,
    update_job,
)
from workers.coordination import WorkerCoordination

DEFAULT_SCHEDULER_INTERVAL_SECONDS = 1.0
TASK_LEASE_DURATION_SECONDS = 30.0
logger = logging.getLogger(__name__)
Record = dict[str, Any]


class Scheduler:
    def __init__(
        self,
        coordination: WorkerCoordination,
        interval_seconds: float = DEFAULT_SCHEDULER_INTERVAL_SECONDS,
    ) -> None:
        if not math.isfinite(interval_seconds) or interval_seconds <= 0:
            raise ValueError("Scheduler interval must be finite and greater than zero.")

        self._coordination = coordination
        self._interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("The Scheduler is already running.")

        self._thread = threading.Thread(
            target=self._run,
            name=f"scheduler-{self._coordination.node_id}",
            daemon=True,
        )
        self._thread.start()
        logger.info("Scheduler started for Worker %s.", self._coordination.node_id)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._schedule_pending_jobs()
            except (PsycopgError, PoolTimeout) as exc:
                logger.warning(
                    "Scheduler for Worker %s could not complete a planning cycle (%s).",
                    self._coordination.node_id,
                    type(exc).__name__,
                )
            self._stop_event.wait(self._interval_seconds)

    def _schedule_pending_jobs(self) -> None:
        epoch = self._current_leader_epoch()
        if epoch is None:
            return

        for job in list_jobs():
            if self._stop_event.is_set():
                return
            if job.get("status") != "pending":
                continue
            if not self._retains_leadership(epoch):
                return
            if not self._schedule_job(job, epoch):
                return

    def _schedule_job(self, job: Record, epoch: int) -> bool:
        job_id = job.get("job_id")
        requested_workers = job.get("requested_workers")
        dataset_id = job.get("dataset_id")
        if (
            not isinstance(job_id, UUID)
            or not isinstance(dataset_id, UUID)
            or not isinstance(requested_workers, int)
            or isinstance(requested_workers, bool)
            or requested_workers <= 0
        ):
            logger.error("Skipping Job with invalid scheduling fields: %s.", job_id)
            return True

        workers = self._eligible_workers(requested_workers)
        if not workers:
            logger.debug("No eligible Workers are currently available for Job %s.", job_id)
            return True

        dataset = get_dataset(dataset_id)
        if dataset is None:
            logger.error("Skipping Job %s because its Dataset is missing.", job_id)
            return True

        tasks = list_tasks_by_job(job_id)
        if not tasks:
            partitions = _build_partitions(dataset.get("row_count"), len(workers))
            if partitions is None:
                logger.error(
                    "Skipping Job %s because its Dataset row count is invalid.",
                    job_id,
                )
                return True
            if not self._retains_leadership(epoch):
                return False
            created_tasks = create_tasks_for_job(
                job_id,
                partitions,
                self._coordination.node_id,
                epoch,
                DEFAULT_CLUSTER_ID,
            )
            if created_tasks is None:
                return self._retains_leadership(epoch)
            tasks = created_tasks

        if len(tasks) > requested_workers:
            logger.error(
                "Skipping Job %s because it has %s Tasks for only %s requested Workers.",
                job_id,
                len(tasks),
                requested_workers,
            )
            return True

        assigned_node_ids = {
            task.get("node_id")
            for task in tasks
            if isinstance(task.get("node_id"), UUID)
        }
        if len(assigned_node_ids) > requested_workers:
            logger.error(
                "Skipping Job %s because its Tasks exceed the requested Worker count.",
                job_id,
            )
            return True

        available_node_ids = [
            worker["node_id"]
            for worker in workers
            if worker["node_id"] not in assigned_node_ids
        ][: max(0, requested_workers - len(assigned_node_ids))]
        pending_tasks = [
            task
            for task in tasks
            if task.get("status") == "pending" and task.get("node_id") is None
        ]

        for task, worker_node_id in zip(pending_tasks, available_node_ids):
            if not self._retains_leadership(epoch):
                return False
            task_id = task.get("task_id")
            if not isinstance(task_id, UUID):
                logger.error("Skipping a Task with an invalid ID in Job %s.", job_id)
                continue

            assigned_task = assign_task(
                task_id,
                worker_node_id,
                self._coordination.node_id,
                epoch,
                TASK_LEASE_DURATION_SECONDS,
                DEFAULT_CLUSTER_ID,
            )
            if assigned_task is None and not self._retains_leadership(epoch):
                return False
            if assigned_task is not None:
                assigned_node_ids.add(worker_node_id)

        if not self._retains_leadership(epoch):
            return False

        scheduled_tasks = list_tasks_by_job(job_id)
        if not scheduled_tasks or any(
            task.get("status") == "pending" or task.get("node_id") is None
            for task in scheduled_tasks
        ):
            return True

        actual_workers = len(
            {
                task["node_id"]
                for task in scheduled_tasks
                if isinstance(task.get("node_id"), UUID)
            }
        )
        if actual_workers == 0 or actual_workers > requested_workers:
            logger.error(
                "Job %s has an invalid assigned Worker count (%s).",
                job_id,
                actual_workers,
            )
            return True

        if not self._retains_leadership(epoch):
            return False
        updated_job = update_job(
            job_id,
            {
                "status": "running",
                "actual_workers": actual_workers,
                "started_at": job.get("started_at") or datetime.now(timezone.utc),
            },
        )
        if updated_job is None:
            logger.error("Job %s disappeared while being scheduled.", job_id)
        else:
            logger.info(
                "Scheduled Job %s with %s Worker(s) at leadership epoch %s.",
                job_id,
                actual_workers,
                epoch,
            )
        return True

    def _eligible_workers(self, requested_workers: int) -> list[Record]:
        eligible: list[Record] = []
        for node in list_nodes():
            node_id = node.get("node_id")
            cpu_available = node.get("cpu_available_cores")
            ram_available = node.get("ram_available_mb")
            active_tasks = node.get("active_tasks")
            if (
                node.get("status") != "online"
                or not isinstance(node_id, UUID)
                or isinstance(cpu_available, bool)
                or not isinstance(cpu_available, (int, float))
                or not math.isfinite(cpu_available)
                or cpu_available <= 0
                or isinstance(ram_available, bool)
                or not isinstance(ram_available, int)
                or ram_available <= 0
                or isinstance(active_tasks, bool)
                or not isinstance(active_tasks, int)
                or active_tasks < 0
            ):
                continue
            eligible.append(node)

        eligible.sort(
            key=lambda node: (
                node["active_tasks"],
                -node["cpu_available_cores"],
                -node["ram_available_mb"],
                str(node["node_id"]),
            )
        )
        return eligible[:requested_workers]

    def _current_leader_epoch(self) -> int | None:
        if not self._coordination.is_leader:
            return None

        leadership = get_leadership(DEFAULT_CLUSTER_ID)
        if (
            leadership is None
            or leadership.get("leader_node_id") != self._coordination.node_id
            or leadership.get("is_valid") is not True
        ):
            return None

        epoch = leadership.get("epoch")
        if not isinstance(epoch, int) or isinstance(epoch, bool):
            logger.error(
                "Leadership for Worker %s has an invalid epoch.",
                self._coordination.node_id,
            )
            return None
        return epoch

    def _retains_leadership(self, epoch: int) -> bool:
        return (
            not self._stop_event.is_set()
            and self._current_leader_epoch() == epoch
        )


def _build_partitions(
    row_count: Any,
    worker_count: int,
) -> list[dict[str, int | None]] | None:
    if worker_count <= 0:
        return []
    if row_count is not None and (
        not isinstance(row_count, int)
        or isinstance(row_count, bool)
        or row_count < 0
    ):
        return None
    if row_count is None:
        rows_per_partition: list[int | None] = [None] * worker_count
    else:
        base_rows, extra_rows = divmod(row_count, worker_count)
        rows_per_partition = [
            base_rows + (1 if index < extra_rows else 0)
            for index in range(worker_count)
        ]
    return [
        {"partition_index": index, "rows_assigned": rows_assigned}
        for index, rows_assigned in enumerate(rows_per_partition)
    ]
