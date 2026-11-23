from datetime import datetime
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

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
    algorithm: str
    target_column: str
    requested_workers: int = Field(gt=0)
    parameters: dict[str, Any] = Field(default_factory=dict)
    train_ratio: float | None = Field(default=None, gt=0, lt=1)
    seed: int | None = None


@router.post("/jobs", status_code=status.HTTP_201_CREATED)
def create_job(job: JobCreate) -> dict[str, Any]:
    if repository.get_dataset(job.dataset_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Dataset not found.",
        )

    return repository.create_job(
        dataset_id=job.dataset_id,
        algorithm=job.algorithm,
        target_column=job.target_column,
        requested_workers=job.requested_workers,
        status="pending",
        parameters=job.parameters,
        train_ratio=job.train_ratio,
        seed=job.seed,
    )


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
