import re
from datetime import datetime
from typing import Any, Literal
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

import dataset_storage
from coordinator.model_export import MODEL_FORMAT
from coordinator.preprocessing import (
    DEFAULT_SEED,
    DEFAULT_TRAIN_RATIO,
    DatasetPreparationError,
    prepare_dataset,
)
from database import repository
from workers.training_engine import DEFAULT_EPOCHS, DEFAULT_LEARNING_RATE, MAX_EPOCHS

router = APIRouter()


class JobResponse(BaseModel):
    job_id: UUID
    dataset_id: UUID
    algorithm: str
    target_column: str
    status: str
    parameters: dict[str, Any]
    # Legacy field: kept nullable for historical Jobs, not used to plan.
    requested_workers: int | None = None
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


class TrainingParameters(BaseModel):
    model_config = ConfigDict(extra="forbid")

    learning_rate: float = Field(default=DEFAULT_LEARNING_RATE, gt=0, le=10)
    epochs: int = Field(default=DEFAULT_EPOCHS, ge=1, le=MAX_EPOCHS)


class JobCreate(BaseModel):
    dataset_id: UUID
    # Only Logistic Regression trains (README: one algorithm). The name
    # follows main's convention.
    algorithm: Literal["logistic-regression"] = "logistic-regression"
    target_column: str = Field(min_length=1)
    parameters: TrainingParameters = Field(default_factory=TrainingParameters)
    train_ratio: float = Field(default=DEFAULT_TRAIN_RATIO, gt=0, lt=1)
    seed: int = DEFAULT_SEED


@router.post("/jobs", status_code=status.HTTP_201_CREATED)
def create_job(job: JobCreate) -> dict[str, Any]:
    dataset = repository.get_dataset(job.dataset_id)
    if dataset is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el dataset.",
        )

    # The Job uses every eligible Worker (contrato.md §17.1): no Worker count
    # is requested here. Running the full preparation here rejects unusable datasets before a
    # Job exists, instead of letting the Coordinator fail it later.
    try:
        csv_path = dataset_storage.resolve_path(dataset["file_path"])
    except ValueError:
        csv_path = None
    if csv_path is None or not csv_path.is_file():
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El archivo del dataset no está disponible en este Backend.",
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

    target = job.target_column.strip()
    headers = dataset_storage.read_columns(dataset["file_path"])
    used_columns = set(prepared.input_columns)
    created = repository.create_job(
        dataset_id=job.dataset_id,
        algorithm=job.algorithm,
        target_column=target,
        status="pending",
        # Stored with defaults filled in, so the Job records exactly what runs,
        # plus the dataset columns as main records them.
        parameters={
            **job.parameters.model_dump(),
            "dataset_headers": headers,
            "feature_columns": [header for header in headers if header in used_columns],
        },
        train_ratio=job.train_ratio,
        seed=job.seed,
    )
    return {**created, "preparation": prepared.summary()}


@router.get("/jobs", response_model=list[JobResponse])
def list_jobs() -> list[dict[str, Any]]:
    """Every Job, newest first."""
    return list(reversed(repository.list_jobs()))


@router.get("/jobs/{job_id}", response_model=JobResponse)
def read_job(job_id: UUID) -> dict[str, Any]:
    job = repository.get_job(job_id)
    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el Job.",
        )
    return job


@router.get("/jobs/{job_id}/tasks", response_model=list[TaskResponse])
def read_job_tasks(job_id: UUID) -> list[dict[str, Any]]:
    if repository.get_job(job_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el Job.",
        )
    return repository.list_tasks_by_job(job_id)


@router.get("/jobs/{job_id}/result")
def read_job_result(job_id: UUID) -> dict[str, Any]:
    if repository.get_job(job_id) is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="No existe el Job.",
        )
    result = repository.get_result_by_job(job_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="El Job todavía no tiene resultado.",
        )
    return result


@router.get("/jobs/{job_id}/model")
def download_job_model(job_id: UUID) -> JSONResponse:
    """The trained model as a JSON file the user takes to their own project."""
    job = repository.get_job(job_id)
    if job is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No existe el Job.")
    result = repository.get_result_by_job(job_id)
    if result is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="El Job todavía no tiene resultado.",
        )
    model = (result.get("metrics") or {}).get("model") or {}
    if model.get("format") != MODEL_FORMAT:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=(
                "Este resultado es anterior al modelo descargable y no guarda la "
                "preparación de los datos. Vuelve a entrenar para obtenerlo."
            ),
        )

    dataset = repository.get_dataset(job["dataset_id"])
    dataset_name = dataset["name"] if dataset else "dataset"
    artifact = {**model, "job_id": str(job_id), "dataset": dataset_name}
    slug = re.sub(r"[^a-z0-9]+", "-", dataset_name.lower()).strip("-") or "dataset"
    filename = f"modelo-{slug}-{str(job_id)[:8]}.json"
    return JSONResponse(
        content=artifact,
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
