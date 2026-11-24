"""Loads the prepared dataset of a Job, with a small in-memory cache.

The Scheduler, the Coordinator and every Worker prepare the same CSV with the
Job's target, train_ratio and seed. Preparation is deterministic, so they all
obtain identical training rows without sending the data over the network.
The CSV must be readable from the process (same host in this deployment).
"""

import threading
from collections import OrderedDict
from typing import Any
from uuid import UUID

import dataset_storage
from coordinator.preprocessing import (
    DEFAULT_SEED,
    DEFAULT_TRAIN_RATIO,
    DatasetPreparationError,
    PreparedDataset,
    prepare_dataset,
)
from database.repository import get_dataset

Record = dict[str, Any]


def load_prepared_dataset(job: Record) -> PreparedDataset:
    dataset = get_dataset(job["dataset_id"])
    if dataset is None:
        raise DatasetPreparationError(f"Dataset {job['dataset_id']} no longer exists.")
    try:
        csv_path = dataset_storage.resolve_path(dataset["file_path"])
    except ValueError as exc:
        raise DatasetPreparationError(str(exc)) from exc

    train_ratio = job.get("train_ratio")
    seed = job.get("seed")
    return prepare_dataset(
        csv_path,
        job["target_column"],
        train_ratio=DEFAULT_TRAIN_RATIO if train_ratio is None else float(train_ratio),
        seed=DEFAULT_SEED if seed is None else int(seed),
    )


class PreparedDatasetCache:
    """Keeps the prepared data of the most recent Jobs (one Job runs at a time)."""

    def __init__(self, max_jobs: int = 2) -> None:
        self._max_jobs = max_jobs
        self._entries: OrderedDict[UUID, PreparedDataset] = OrderedDict()
        self._lock = threading.Lock()

    def get(self, job: Record) -> PreparedDataset:
        job_id = job["job_id"]
        with self._lock:
            cached = self._entries.get(job_id)
            if cached is not None:
                self._entries.move_to_end(job_id)
                return cached
            # Preparing under the lock avoids parallel requests repeating it.
            prepared = load_prepared_dataset(job)
            self._entries[job_id] = prepared
            while len(self._entries) > self._max_jobs:
                self._entries.popitem(last=False)
            return prepared
