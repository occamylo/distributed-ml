from typing import Any

from fastapi import APIRouter, status
from pydantic import BaseModel

from database import repository

router = APIRouter()


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
