"""Local file storage and validation for uploaded CSV datasets.

CockroachDB keeps only the metadata and the relative path of each file; the
CSV content lives on the Backend host under ``storage/datasets``.
"""

import csv
import os
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO
from uuid import uuid4

BACKEND_DIR = Path(__file__).resolve().parent
DATASETS_DIR = BACKEND_DIR / "storage" / "datasets"
RELATIVE_DATASETS_DIR = PurePosixPath("storage", "datasets")

MAX_DATASET_BYTES = int(os.environ.get("DATASET_MAX_BYTES", 100 * 1024 * 1024))
MIN_COLUMNS = 2
MIN_DATA_ROWS = 2
MAX_COLUMN_NAME_LENGTH = 255
_CHUNK_BYTES = 1024 * 1024


class DatasetValidationError(ValueError):
    """The uploaded file is not a CSV the pipeline can use."""


@dataclass(frozen=True)
class StoredDataset:
    relative_path: str
    absolute_path: Path
    file_size_bytes: int
    row_count: int
    column_count: int
    columns: list[str]


def store_csv(source: BinaryIO, original_filename: str | None) -> StoredDataset:
    """Copies an uploaded CSV to local storage and validates it.

    The file is removed again if validation fails, so callers only see files
    that passed every check.
    """
    _validate_extension(original_filename)
    DATASETS_DIR.mkdir(parents=True, exist_ok=True)
    file_name = f"{uuid4()}.csv"
    absolute_path = DATASETS_DIR / file_name

    try:
        file_size_bytes = _copy_with_limit(source, absolute_path)
        columns, row_count = _inspect_csv(absolute_path)
    except BaseException:
        absolute_path.unlink(missing_ok=True)
        raise

    return StoredDataset(
        relative_path=str(RELATIVE_DATASETS_DIR / file_name),
        absolute_path=absolute_path,
        file_size_bytes=file_size_bytes,
        row_count=row_count,
        column_count=len(columns),
        columns=columns,
    )


def resolve_path(relative_path: str) -> Path:
    """Returns the absolute path of a stored dataset, confined to storage."""
    candidate = (BACKEND_DIR / relative_path).resolve()
    if not candidate.is_relative_to(DATASETS_DIR.resolve()):
        raise ValueError("Dataset path is outside the datasets storage directory.")
    return candidate


def read_columns(relative_path: str) -> list[str]:
    """Reads the header of a stored dataset."""
    path = resolve_path(relative_path)
    with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
        header = next(csv.reader(csv_file), None)
    if header is None:
        raise DatasetValidationError("The stored dataset has no header row.")
    return [column.strip() for column in header]


def delete_file(relative_path: str) -> None:
    """Removes a stored dataset file; a missing file is not an error."""
    resolve_path(relative_path).unlink(missing_ok=True)


def _validate_extension(original_filename: str | None) -> None:
    if not original_filename or not original_filename.lower().endswith(".csv"):
        raise DatasetValidationError("The file must have a .csv extension.")


def _copy_with_limit(source: BinaryIO, destination: Path) -> int:
    written = 0
    with destination.open("xb") as target:
        while chunk := source.read(_CHUNK_BYTES):
            written += len(chunk)
            if written > MAX_DATASET_BYTES:
                raise DatasetValidationError(
                    f"The file exceeds the {MAX_DATASET_BYTES // (1024 * 1024)} MB limit."
                )
            target.write(chunk)
    if written == 0:
        raise DatasetValidationError("The file is empty.")
    return written


def _inspect_csv(path: Path) -> tuple[list[str], int]:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.reader(csv_file, strict=True)
            header = next(reader, None)
            columns = _validate_header(header)

            row_count = 0
            for row in reader:
                if not any(value.strip() for value in row):
                    continue
                if len(row) != len(columns):
                    raise DatasetValidationError(
                        f"Line {reader.line_num} has {len(row)} values; "
                        f"the header has {len(columns)} columns."
                    )
                row_count += 1
    except UnicodeDecodeError as exc:
        raise DatasetValidationError("The file must be encoded as UTF-8.") from exc
    except csv.Error as exc:
        raise DatasetValidationError(f"The file is not a valid CSV ({exc}).") from exc

    if row_count < MIN_DATA_ROWS:
        raise DatasetValidationError(
            f"The file needs at least {MIN_DATA_ROWS} data rows; it has {row_count}."
        )
    return columns, row_count


def _validate_header(header: list[str] | None) -> list[str]:
    if header is None:
        raise DatasetValidationError("The file has no header row.")

    columns = [column.strip() for column in header]
    if len(columns) < MIN_COLUMNS:
        raise DatasetValidationError(
            f"The file needs at least {MIN_COLUMNS} comma-separated columns "
            "(features and a target)."
        )
    if any(not column for column in columns):
        raise DatasetValidationError("Every column in the header needs a name.")
    if any(len(column) > MAX_COLUMN_NAME_LENGTH for column in columns):
        raise DatasetValidationError(
            f"Column names can have at most {MAX_COLUMN_NAME_LENGTH} characters."
        )

    duplicates = sorted({column for column in columns if columns.count(column) > 1})
    if duplicates:
        raise DatasetValidationError(
            "Column names must be unique; repeated: " + ", ".join(duplicates) + "."
        )
    return columns
