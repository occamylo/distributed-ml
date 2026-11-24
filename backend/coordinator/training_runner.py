"""Runs the distributed training loop on the Worker that holds the lease.

For each ``running`` Job whose Tasks are all assigned, the coordinator:

1. Marks the Tasks ``running`` (``started_at``), fenced by its leadership.
2. For every training epoch, sends the current global weights to all Tasks,
   waits for every gradient, aggregates them with the weighted average
   G = sum(n_i * G_i) / sum(n_i) and updates w <- w - learning_rate * G.
3. Evaluates the final weights on the test split, which never took part in
   training, and in one fenced transaction marks the Tasks and the Job
   ``completed`` and stores the single ``results`` row.

The epoch context (weights, loss history) lives only in this process, as
the data contract requires: Tasks are logical partitions, not per-epoch
rows. Every state change goes through a repository operation that checks
the leader and its epoch in the same transaction. If this Worker loses the
lease mid-training it stops; the next coordinator restarts the Job from
the initial weights, which is deterministic and gives the same model.

Not yet here: recovery of failed Tasks (Tarea 18). Until then a failed
round fails the Job.
"""

import logging
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from psycopg import Error as PsycopgError
from psycopg_pool import PoolTimeout

from coordinator.aggregator import aggregate_gradients, apply_update
from coordinator.dispatcher import RoundFailedError, request_gradients
from coordinator.job_data import PreparedDatasetCache
from coordinator.preprocessing import DatasetPreparationError, PreparedDataset
from database.repository import (
    DEFAULT_CLUSTER_ID,
    complete_job_as_leader,
    get_job,
    get_leadership,
    list_jobs,
    list_nodes,
    list_tasks_by_job,
    update_job_as_leader,
    update_job_tasks_as_leader,
)
from workers.coordination import WorkerCoordination
from workers.training_engine import (
    DEFAULT_EPOCHS,
    DEFAULT_LEARNING_RATE,
    Evaluation,
    ModelParameters,
    evaluate,
    initial_parameters,
    validate_hyperparameters,
)

DEFAULT_RUNNER_INTERVAL_SECONDS = 1.0
LOG_EVERY_EPOCHS = 10
MAX_LOSS_HISTORY_POINTS = 200
logger = logging.getLogger(__name__)
Record = dict[str, Any]


@dataclass(frozen=True)
class TrainingSummary:
    learning_rate: float
    loss_history: list[float]
    partitions: int
    workers: int
    duration_seconds: float


def build_result_metrics(
    prepared: PreparedDataset,
    weights: ModelParameters,
    train_evaluation: Evaluation,
    test_evaluation: Evaluation,
    summary: TrainingSummary,
) -> dict[str, Any]:
    """JSON document stored in ``results.metrics``.

    Holds what the result screen and the local vs distributed comparison
    need: both evaluations, the training configuration actually used, a
    sampled loss curve and the final weights (small for Logistic
    Regression, so no separate model file is written).
    """
    history = summary.loss_history
    step = max(1, -(-len(history) // MAX_LOSS_HISTORY_POINTS))
    sampled = list(range(0, len(history), step))
    if history and sampled[-1] != len(history) - 1:
        sampled.append(len(history) - 1)
    return {
        "class_labels": prepared.class_labels,
        "feature_names": prepared.feature_names,
        "test": test_evaluation.to_dict(prepared.class_labels),
        "train": train_evaluation.to_dict(prepared.class_labels),
        "training": {
            "epochs": len(history),
            "learning_rate": summary.learning_rate,
            "partitions": summary.partitions,
            "workers": summary.workers,
            "train_rows": len(prepared.y_train),
            "test_rows": len(prepared.y_test),
        },
        "train_loss_by_epoch": [[index + 1, history[index]] for index in sampled],
        "model": weights.to_dict(),
    }


class LeadershipLost(Exception):
    """This Worker is no longer the coordinator at the epoch it started with."""


class TrainingCoordinator:
    def __init__(
        self,
        coordination: WorkerCoordination,
        interval_seconds: float = DEFAULT_RUNNER_INTERVAL_SECONDS,
    ) -> None:
        if interval_seconds <= 0:
            raise ValueError("The training runner interval must be greater than zero.")
        self._coordination = coordination
        self._interval_seconds = interval_seconds
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._prepared_datasets = PreparedDatasetCache()

    def start(self) -> None:
        if self._thread is not None:
            raise RuntimeError("The training coordinator is already running.")
        self._thread = threading.Thread(
            target=self._run,
            name=f"training-{self._coordination.node_id}",
            daemon=True,
        )
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread is not None:
            self._thread.join()

    def _run(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._train_next_job()
            except LeadershipLost:
                logger.warning(
                    "Worker %s stopped training because it lost the coordinator lease.",
                    self._coordination.node_id,
                )
            except (PsycopgError, PoolTimeout) as exc:
                logger.warning(
                    "Training coordinator on Worker %s could not complete a cycle (%s).",
                    self._coordination.node_id,
                    type(exc).__name__,
                )
            self._stop_event.wait(self._interval_seconds)

    def _train_next_job(self) -> None:
        epoch = self._current_leader_epoch()
        if epoch is None:
            return
        # One Job at a time, oldest first (list_jobs orders by created_at).
        for job in list_jobs():
            if job.get("status") != "running":
                continue
            tasks = list_tasks_by_job(job["job_id"])
            if tasks and all(task.get("status") in ("assigned", "running") for task in tasks):
                self.train_job(job, epoch)
                return

    def train_job(self, job: Record, leader_epoch: int) -> None:
        job_id = job["job_id"]
        started = time.perf_counter()

        try:
            prepared = self._prepared_datasets.get(job)
        except DatasetPreparationError as exc:
            self._fail_job(job_id, leader_epoch, f"Dataset preparation failed: {exc}")
            return

        parameters = job.get("parameters") or {}
        learning_rate = float(parameters.get("learning_rate", DEFAULT_LEARNING_RATE))
        epochs = parameters.get("epochs", DEFAULT_EPOCHS)
        try:
            validate_hyperparameters(learning_rate, epochs)
        except ValueError as exc:
            self._fail_job(job_id, leader_epoch, f"Invalid training parameters: {exc}")
            return

        started_tasks = update_job_tasks_as_leader(
            job_id,
            ["assigned"],
            {"status": "running", "started_at": datetime.now(timezone.utc)},
            self._coordination.node_id,
            leader_epoch,
            DEFAULT_CLUSTER_ID,
        )
        if started_tasks is None:
            raise LeadershipLost()

        weights = initial_parameters(len(prepared.feature_names), len(prepared.class_labels))
        logger.info(
            "Training Job %s: %s epochs, learning_rate %s, %s training rows.",
            job_id,
            epochs,
            learning_rate,
            len(prepared.y_train),
        )

        loss_history: list[float] = []
        for training_epoch in range(1, epochs + 1):
            if self._stop_event.is_set():
                return
            self._ensure_leadership(leader_epoch)
            current = get_job(job_id)
            if current is None or current.get("status") != "running":
                logger.info("Job %s is no longer running; training stopped.", job_id)
                return

            # Tasks are re-read every epoch so a reassignment (Tarea 18)
            # takes effect in the next round.
            tasks = list_tasks_by_job(job_id)
            nodes_by_id = {node["node_id"]: node for node in list_nodes()}
            try:
                results = request_gradients(
                    tasks, nodes_by_id, weights, leader_epoch, training_epoch
                )
            except RoundFailedError as exc:
                self._ensure_leadership(leader_epoch)
                self._fail_job(
                    job_id,
                    leader_epoch,
                    f"Training epoch {training_epoch} could not collect every gradient: {exc}",
                )
                return

            gradient = aggregate_gradients([result.gradient for result in results])
            weights = apply_update(weights, gradient, learning_rate)
            loss_history.append(gradient.loss)
            if training_epoch == 1 or training_epoch % LOG_EVERY_EPOCHS == 0 or training_epoch == epochs:
                logger.info(
                    "Job %s epoch %s/%s: train loss %.6f over %s rows from %s Task(s).",
                    job_id,
                    training_epoch,
                    epochs,
                    gradient.loss,
                    gradient.sample_count,
                    len(results),
                )

        self._ensure_leadership(leader_epoch)
        self._complete_job(
            job_id,
            leader_epoch,
            prepared,
            weights,
            TrainingSummary(
                learning_rate=learning_rate,
                loss_history=loss_history,
                partitions=len(tasks),
                workers=len({task["node_id"] for task in tasks}),
                duration_seconds=time.perf_counter() - started,
            ),
        )

    def _complete_job(
        self,
        job_id: UUID,
        leader_epoch: int,
        prepared: PreparedDataset,
        weights: ModelParameters,
        summary: "TrainingSummary",
    ) -> None:
        # The test split never took part in training (README, step 11).
        train_evaluation = evaluate(weights, prepared.x_train, prepared.y_train)
        test_evaluation = evaluate(weights, prepared.x_test, prepared.y_test)
        metrics = build_result_metrics(
            prepared, weights, train_evaluation, test_evaluation, summary
        )
        result = complete_job_as_leader(
            job_id,
            completed_at=datetime.now(timezone.utc),
            duration_seconds=summary.duration_seconds,
            accuracy=test_evaluation.accuracy,
            loss=test_evaluation.loss,
            metrics=metrics,
            leader_node_id=self._coordination.node_id,
            leader_epoch=leader_epoch,
        )
        if result is None:
            # Either the lease moved or the Job left ``running`` meanwhile.
            self._ensure_leadership(leader_epoch)
            logger.warning("Job %s was no longer running; its result was not stored.", job_id)
            return
        logger.info(
            "Job %s completed: %s epochs in %.3f s; test accuracy %.4f, test loss %.6f.",
            job_id,
            len(summary.loss_history),
            summary.duration_seconds,
            test_evaluation.accuracy,
            test_evaluation.loss,
        )

    def _fail_job(self, job_id: UUID, leader_epoch: int, reason: str) -> None:
        now = datetime.now(timezone.utc)
        reason = reason[:1000]
        update_job_tasks_as_leader(
            job_id,
            ["assigned", "running"],
            {"status": "failed", "completed_at": now, "error_message": reason},
            self._coordination.node_id,
            leader_epoch,
            DEFAULT_CLUSTER_ID,
        )
        job = update_job_as_leader(
            job_id,
            "running",
            {"status": "failed", "completed_at": now, "error_message": reason},
            self._coordination.node_id,
            leader_epoch,
            DEFAULT_CLUSTER_ID,
        )
        if job is not None:
            logger.error("Job %s failed: %s", job_id, reason)

    def _ensure_leadership(self, leader_epoch: int) -> None:
        if self._current_leader_epoch() != leader_epoch:
            raise LeadershipLost()

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
        return epoch if isinstance(epoch, int) and not isinstance(epoch, bool) else None
