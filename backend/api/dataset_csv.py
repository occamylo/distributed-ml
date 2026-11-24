import csv
import math
from dataclasses import dataclass
from pathlib import Path


_MISSING_VALUE_MARKERS = {"", "na", "n/a", "nan", "null"}


class DatasetValidationError(ValueError):
    pass


@dataclass(frozen=True)
class CSVMetadata:
    headers: tuple[str, ...]
    row_count: int
    column_count: int


def dataset_storage_directory() -> Path:
    return Path(__file__).resolve().parents[1] / "storage" / "datasets"


def resolve_uploaded_dataset_path(file_path: str) -> Path:
    storage_root = dataset_storage_directory().resolve()
    path = Path(file_path).resolve()
    try:
        path.relative_to(storage_root)
    except ValueError as exc:
        raise DatasetValidationError(
            "El dataset no está en el almacenamiento administrado por la aplicación."
        ) from exc

    if not path.is_file():
        raise DatasetValidationError("No se encontró el archivo CSV del dataset.")
    return path


def inspect_csv(
    path: Path,
    *,
    target_column: str | None = None,
    algorithm: str | None = None,
) -> CSVMetadata:
    try:
        with path.open("r", encoding="utf-8-sig", newline="") as source:
            reader = csv.reader(source, strict=True)
            raw_headers = next(reader, None)
            if raw_headers is None:
                raise DatasetValidationError("El archivo CSV está vacío.")

            headers = tuple(raw_headers)
            if len(headers) < 2:
                raise DatasetValidationError(
                    "El CSV debe contener al menos una columna de características y una columna objetivo."
                )
            if any(not header.strip() for header in headers):
                raise DatasetValidationError("Todos los encabezados deben tener un nombre.")
            if len({header.strip().casefold() for header in headers}) != len(headers):
                raise DatasetValidationError("Los encabezados del CSV deben ser únicos.")

            target_index = None
            if target_column is not None:
                try:
                    target_index = headers.index(target_column)
                except ValueError as exc:
                    raise DatasetValidationError(
                        "La columna objetivo seleccionada no existe en el CSV."
                    ) from exc

            row_count = 0
            target_values: set[str] = set()
            for record_number, row in enumerate(reader, start=2):
                if not row:
                    raise DatasetValidationError(
                        f"El registro {record_number} está vacío."
                    )
                if len(row) != len(headers):
                    raise DatasetValidationError(
                        f"El registro {record_number} contiene {len(row)} columnas; "
                        f"se esperaban {len(headers)}."
                    )

                for column_index, value in enumerate(row):
                    if value.strip().casefold() in _MISSING_VALUE_MARKERS:
                        raise DatasetValidationError(
                            f"Hay un dato faltante en el registro {record_number}, "
                            f"columna «{headers[column_index]}»."
                        )

                if target_index is not None:
                    value = row[target_index]
                    if algorithm == "linear-regression":
                        try:
                            numeric_value = float(value.strip())
                        except ValueError as exc:
                            raise DatasetValidationError(
                                "La regresión lineal requiere un target numérico."
                            ) from exc
                        if not math.isfinite(numeric_value):
                            raise DatasetValidationError(
                                "El target de regresión lineal debe contener números finitos."
                            )
                    elif algorithm == "logistic-regression":
                        if len(target_values) < 2:
                            target_values.add(value)

                row_count += 1

    except UnicodeDecodeError as exc:
        raise DatasetValidationError(
            "El archivo debe ser un CSV de texto codificado en UTF-8."
        ) from exc
    except csv.Error as exc:
        raise DatasetValidationError("El archivo no contiene un CSV legible.") from exc

    if row_count < 2:
        raise DatasetValidationError(
            "El CSV debe contener al menos dos registros de datos."
        )
    if (
        algorithm == "logistic-regression"
        and target_column is not None
        and len(target_values) < 2
    ):
        raise DatasetValidationError(
            "La columna objetivo debe contener al menos dos valores distintos "
            "para el modelo seleccionado."
        )

    return CSVMetadata(
        headers=headers,
        row_count=row_count,
        column_count=len(headers),
    )
