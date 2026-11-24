import logging
from pathlib import Path
from typing import Annotated, Any
from uuid import UUID

from fastapi import APIRouter, File, Form, HTTPException, UploadFile, status
from fastapi.concurrency import run_in_threadpool
from psycopg.errors import ForeignKeyViolation

import dataset_storage
from database import repository

router = APIRouter()
logger = logging.getLogger(__name__)

MAX_NAME_LENGTH = 255


@router.post("/datasets", status_code=status.HTTP_201_CREATED)
async def upload_dataset(
    file: Annotated[UploadFile, File(description="CSV file with a header row.")],
    name: Annotated[str | None, Form(max_length=MAX_NAME_LENGTH)] = None,
) -> dict[str, Any]:
    try:
        stored = await run_in_threadpool(
            dataset_storage.store_csv, file.file, file.filename
        )
    except dataset_storage.DatasetValidationError as exc:
        raise HTTPException(
            status_code=422,
            detail=str(exc),
        ) from exc
    finally:
        await file.close()

    dataset_name = (name or "").strip() or Path(file.filename or "dataset.csv").stem
    try:
        dataset = await run_in_threadpool(
            repository.create_dataset,
            name=dataset_name[:MAX_NAME_LENGTH],
            file_path=stored.relative_path,
            file_size_bytes=stored.file_size_bytes,
            row_count=stored.row_count,
            column_count=stored.column_count,
            file_format="csv",
        )
    except BaseException:
        # Without a metadata row the file would be orphaned.
        stored.absolute_path.unlink(missing_ok=True)
        raise

    return {**dataset, "columns": stored.columns}


@router.get("/datasets")
def list_datasets() -> list[dict[str, Any]]:
    return repository.list_datasets()


@router.get("/datasets/{dataset_id}")
def read_dataset(dataset_id: UUID) -> dict[str, Any]:
    return _get_dataset_or_404(dataset_id)


@router.get("/datasets/{dataset_id}/columns")
def read_dataset_columns(dataset_id: UUID) -> dict[str, Any]:
    dataset = _get_dataset_or_404(dataset_id)
    try:
        columns = dataset_storage.read_columns(dataset["file_path"])
    except (OSError, ValueError) as exc:
        logger.warning("Dataset %s file could not be read (%s).", dataset_id, exc)
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="El archivo del dataset no está o no se puede leer en este Backend.",
        ) from exc
    return {"dataset_id": dataset_id, "columns": columns}


@router.delete("/datasets/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_dataset(dataset_id: UUID) -> None:
    dataset = _get_dataset_or_404(dataset_id)
    try:
        deleted = repository.delete_dataset(dataset_id)
    except ForeignKeyViolation as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="No se puede borrar: uno o más Jobs usan este dataset.",
        ) from exc
    if not deleted:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No existe el dataset.")

    try:
        dataset_storage.delete_file(dataset["file_path"])
    except (OSError, ValueError) as exc:
        logger.warning("Dataset %s file could not be removed (%s).", dataset_id, exc)


def _get_dataset_or_404(dataset_id: UUID) -> dict[str, Any]:
    dataset = repository.get_dataset(dataset_id)
    if dataset is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="No existe el dataset.")
    return dataset
