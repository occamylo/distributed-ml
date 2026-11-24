from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

import dataset_storage
from coordinator.preprocessing import (
    DEFAULT_SEED,
    DEFAULT_TRAIN_RATIO,
    DatasetPreparationError,
    prepare_dataset,
)
from database import repository

router = APIRouter()


class JobResponse(BaseModel):
    job_id: UUID
    dataset_id: UUID
    algorithm: str
    target_column: str
    status: str
    parameters: dict[str, Any]
    requested_workers: int
    actual_workers: int | None
    train_ratio: float | None
    seed: int | None
    error_message: str | None
    created_at: datetime
    started_at: datetime | None
    completed_at: datetime | None


class TaskResponse(BaseModel):
    task_id: UUID
    job_id: UUID
    node_id: UUID | None
    partition_index: int
    rows_assigned: int | None
    status: str
    attempts: int
    attempt_id: UUID | None = None
    lease_until: datetime | None = None
    leader_epoch: int | None = None
    started_at: datetime | None
    completed_at: datetime | None
    error_message: str | None


class JobCreate(BaseModel):
    dataset_id: UUID
    algorithm: Literal["logistic_regression"] = "logistic_regression"
    target_column: str = Field(min_length=1)
    requested_workers: int = Field(gt=0)
    parameters: dict[str, Any] = Field(default_factory=dict)
    train_ratio: float = Field(default=DEFAULT_TRAIN_RATIO, gt=0, lt=1)
    seed: int = DEFAULT_SEED


@router.post("/jobs", status_code=status.HTTP_201_CREATED)
def create_job(job: JobCreate) -> dict[str, Any]:
    dataset = repository.get_dataset(job.dataset_id)
    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found.",
        )

    # Running the full preparation here rejects unusable datasets before a
    # Job exists, instead of letting the Coordinator fail it later.
    try:
        csv_path = dataset_storage.resolve_path(dataset["file_path"])
    except ValueError:
        csv_path = None
    if csv_path is None or not csv_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="The dataset file is not available on this Backend host.",
        )

    try:
        prepared = prepare_dataset(
            csv_path,
            job.target_column,
            train_ratio=job.train_ratio,
            seed=job.seed,
        )
    except DatasetPreparationError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc

    created = repository.create_job(
        dataset_id=job.dataset_id,
        algorithm=job.algorithm,
        target_column=job.target_column.strip(),
        requested_workers=job.requested_workers,
        status="pending",
        parameters=job.parameters,
        train_ratio=job.train_ratio,
        seed=job.seed,
    )
    return {**created, "preparation": prepared.summary()}


@router.get("/jobs/{job_id}", response_model=JobResponse)
def read_job(job_id: UUID) -> dict[str, Any]:
    job = repository.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found.",
        )
    return job


@router.get("/jobs/{job_id}/tasks", response_model=list[TaskResponse])
def read_job_tasks(job_id: UUID) -> list[dict[str, Any]]:
    if repository.get_job(job_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found.",
        )
    return repository.list_tasks_by_job(job_id)
