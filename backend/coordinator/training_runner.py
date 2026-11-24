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

If a Task fails, the gradients that did arrive are kept and only the failed
Tasks are retried in the same epoch, with the same weights: the coordinator
waits for the failed Worker's Heartbeat to expire, marks it offline and
reassigns its Tasks to the survivors as new attempts (``recovery.py``). After
the retry limit, or without Workers alive, the Job fails.
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
from coordinator.dispatcher import TaskFailure, TaskGradient, dispatch_round
from coordinator.job_data import PreparedDatasetCache
from coordinator.model_export import build_model_artifact
from coordinator.preprocessing import DatasetPreparationError, PreparedDataset
from coordinator.recovery import MAX_TASK_ATTEMPTS, RecoveryError, plan_reassignments
from coordinator.scheduler import TASK_LEASE_DURATION_SECONDS
from database.repository import (
    DEFAULT_CLUSTER_ID,
    WORKER_STALE_AFTER_SECONDS,
    complete_job_as_leader,
    get_job,
    get_leadership,
    list_jobs,
    list_nodes,
    list_nodes_with_fresh_metrics,
    list_tasks_by_job,
    mark_stale_nodes_offline,
    reassign_task_as_leader,
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
MODEL_ALGORITHM = "logistic-regression"
# Extra seconds after the Heartbeat timeout before a silent Worker counts as down.
DETECTION_MARGIN_SECONDS = 3.0
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
    sampled loss curve and, under ``model``, the downloadable model artifact
    (preprocessing plus final weights; see ``coordinator/model_export.py``).
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
        "model": build_model_artifact(
            prepared,
            weights,
            algorithm=MODEL_ALGORITHM,
            learning_rate=summary.learning_rate,
            epochs=len(history),
            test_accuracy=test_evaluation.accuracy,
        ),
    }


class LeadershipLost(Exception):
    """This Worker is no longer the coordinator at the epoch it started with."""


class TrainingStopped(Exception):
    """The Worker process is shutting down."""


def _describe_failures(failures: list[TaskFailure]) -> str:
    return "; ".join(f"Task {f.task_id} en el Worker {f.node_id}: {f.reason}" for f in failures)[:800]


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
            except TrainingStopped:
                return
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
            self._fail_job(job_id, leader_epoch, f"No se pudo preparar el dataset: {exc}")
            return

        parameters = job.get("parameters") or {}
        learning_rate = float(parameters.get("learning_rate", DEFAULT_LEARNING_RATE))
        epochs = parameters.get("epochs", DEFAULT_EPOCHS)
        try:
            validate_hyperparameters(learning_rate, epochs)
        except ValueError as exc:
            self._fail_job(job_id, leader_epoch, f"Parámetros de entrenamiento inválidos: {exc}")
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

            collected = self._collect_epoch_gradients(
                job_id, leader_epoch, weights, training_epoch
            )
            if collected is None:
                return
            results, tasks = collected

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

    def _collect_epoch_gradients(
        self,
        job_id: UUID,
        leader_epoch: int,
        weights: ModelParameters,
        training_epoch: int,
    ) -> tuple[list[TaskGradient], list[Record]] | None:
        """Returns one valid gradient per Task, recovering failed Tasks.

        Valid gradients are kept; only the failed Tasks are retried, on the
        surviving Workers, with the same weights and training epoch
        (README, step 10). Returns ``None`` after failing the Job when the
        failure cannot be recovered.
        """
        tasks = list_tasks_by_job(job_id)
        collected: dict[UUID, TaskGradient] = {}
        pending = tasks
        while True:
            nodes_by_id = {node["node_id"]: node for node in list_nodes()}
            outcome = dispatch_round(pending, nodes_by_id, weights, leader_epoch, training_epoch)
            # One accepted gradient per logical Task: a retried Task keeps
            # its task_id, so it can never be counted twice.
            collected.update(outcome.gradients)
            if not outcome.failures:
                tasks = list_tasks_by_job(job_id)
                return [collected[task["task_id"]] for task in tasks], tasks

            self._ensure_leadership(leader_epoch)
            fatal = [failure for failure in outcome.failures if not failure.retryable]
            if fatal:
                self._fail_job(
                    job_id,
                    leader_epoch,
                    f"La época {training_epoch} tuvo una falla de Task no recuperable: "
                    + _describe_failures(fatal),
                )
                return None
            try:
                pending = self._recover_tasks(
                    job_id, leader_epoch, outcome.failures, training_epoch
                )
            except RecoveryError as exc:
                self._fail_job(
                    job_id,
                    leader_epoch,
                    f"La época {training_epoch} no pudo recuperar sus Tasks: {exc} "
                    f"Últimas fallas: {_describe_failures(outcome.failures)}",
                )
                return None

    def _recover_tasks(
        self,
        job_id: UUID,
        leader_epoch: int,
        failures: list[TaskFailure],
        training_epoch: int,
    ) -> list[Record]:
        """Confirms the failed Workers are down and reassigns their Tasks."""
        failed_nodes = {failure.node_id for failure in failures}
        logger.warning(
            "Job %s epoch %s: %s Task(s) failed on Worker(s) %s; waiting for their "
            "Heartbeats to expire before reassigning.",
            job_id,
            training_epoch,
            len(failures),
            ", ".join(str(node) for node in failed_nodes),
        )

        # Detection follows the Heartbeat timeout (README step 10, architecture
        # §8.A): a Worker that keeps publishing gets the rest of the window to
        # recover; after it, its Tasks are reassigned anyway.
        deadline = time.monotonic() + WORKER_STALE_AFTER_SECONDS + DETECTION_MARGIN_SECONDS
        while True:
            self._ensure_leadership(leader_epoch)
            alive_nodes = self._alive_nodes()
            if not failed_nodes & {node["node_id"] for node in alive_nodes}:
                break
            if time.monotonic() >= deadline:
                logger.warning(
                    "Worker(s) %s still publish Heartbeats but do not answer; reassigning.",
                    ", ".join(str(node) for node in failed_nodes),
                )
                break
            if self._stop_event.wait(1.0):
                raise TrainingStopped()

        marked = mark_stale_nodes_offline()
        if marked:
            logger.warning(
                "Marked offline: %s.", ", ".join(str(node["node_id"]) for node in marked)
            )

        job_tasks = list_tasks_by_job(job_id)
        by_id = {task["task_id"]: task for task in job_tasks}
        failed_tasks = [by_id[failure.task_id] for failure in failures]
        alive_ids = [
            node["node_id"] for node in alive_nodes if node["node_id"] not in failed_nodes
        ]
        plan = plan_reassignments(
            failed_tasks, job_tasks, alive_ids, self._coordination.node_id
        )

        reassigned: list[Record] = []
        for task, node_id in plan:
            updated = reassign_task_as_leader(
                task["task_id"],
                task["attempt_id"],
                node_id,
                self._coordination.node_id,
                leader_epoch,
                TASK_LEASE_DURATION_SECONDS,
                DEFAULT_CLUSTER_ID,
            )
            if updated is None:
                self._ensure_leadership(leader_epoch)
                raise RecoveryError(
                    f"La partición {task['partition_index']} cambió mientras se reasignaba."
                )
            logger.warning(
                "Job %s: partition %s reassigned from Worker %s to Worker %s "
                "(attempt %s of %s).",
                job_id,
                task["partition_index"],
                task["node_id"],
                node_id,
                updated["attempts"],
                MAX_TASK_ATTEMPTS,
            )
            reassigned.append(updated)
        return reassigned

    @staticmethod
    def _alive_nodes() -> list[Record]:
        """Online Workers with a fresh Heartbeat, in the Scheduler's order."""
        alive = [
            node
            for node in list_nodes_with_fresh_metrics()
            if node.get("status") in ("online", "busy")
        ]
        alive.sort(
            key=lambda node: (
                node.get("active_tasks") or 0,
                -(node.get("cpu_available_cores") or 0),
                -(node.get("ram_available_mb") or 0),
                str(node["node_id"]),
            )
        )
        return alive

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
