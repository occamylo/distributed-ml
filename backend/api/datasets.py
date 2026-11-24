from pathlib import Path
from typing import Any, BinaryIO
from uuid import uuid4

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.concurrency import run_in_threadpool
from pydantic import BaseModel

from api.dataset_csv import (
    DatasetValidationError,
    dataset_storage_directory,
    inspect_csv,
)
from database import repository

router = APIRouter()


def _create_unique_dataset_file(storage_directory: Path) -> tuple[Path, BinaryIO]:
    while True:
        path = storage_directory / f"{uuid4().hex}.csv"
        try:
            return path, path.open("xb")
        except FileExistsError:
            continue


class DatasetCreate(BaseModel):
    name: str
    file_path: str
    file_size_bytes: int | None = None
    row_count: int | None = None
    column_count: int | None = None
    file_format: str = "csv"


@router.post("/datasets", status_code=status.HTTP_201_CREATED)
def create_dataset(dataset: DatasetCreate) -> dict[str, Any]:
    return repository.create_dataset(
        name=dataset.name,
        file_path=dataset.file_path,
        file_size_bytes=dataset.file_size_bytes,
        row_count=dataset.row_count,
        column_count=dataset.column_count,
        file_format=dataset.file_format,
    )


@router.post("/datasets/upload", status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    request: Request,
    filename: str = Query(min_length=1),
) -> dict[str, Any]:
    safe_filename = filename.strip().replace("\\", "/").rsplit("/", 1)[-1]
    if not safe_filename.lower().endswith(".csv"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Selecciona un archivo con extensión .csv.",
        )

    dataset_name = safe_filename[:-4].strip()
    if not dataset_name:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="El archivo CSV debe tener un nombre.",
        )

    storage_directory = dataset_storage_directory()
    storage_directory.mkdir(parents=True, exist_ok=True)
    stored_path: Path | None = None
    dataset_saved = False

    try:
        stored_path, destination = _create_unique_dataset_file(storage_directory)
        with destination:
            async for chunk in request.stream():
                destination.write(chunk)

        try:
            metadata = inspect_csv(stored_path)
        except DatasetValidationError as exc:
            raise HTTPException(
                status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
                detail=str(exc),
            ) from exc

        file_size_bytes = stored_path.stat().st_size
        dataset = await run_in_threadpool(
            repository.create_dataset,
            name=dataset_name,
            file_path=str(stored_path.resolve()),
            file_size_bytes=file_size_bytes,
            row_count=metadata.row_count,
            column_count=metadata.column_count,
            file_format="csv",
        )
        dataset_saved = True
        return {
            "dataset_id": dataset["dataset_id"],
            "name": dataset["name"],
            "file_size_bytes": file_size_bytes,
            "row_count": metadata.row_count,
            "column_count": metadata.column_count,
            "file_format": "csv",
            "headers": list(metadata.headers),
        }
    finally:
        if stored_path is not None and not dataset_saved:
            stored_path.unlink(missing_ok=True)
