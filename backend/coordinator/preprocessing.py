"""Turns a stored CSV into train/test matrices for Logistic Regression.

Everything the pipeline learns (category lists, means, standard deviations)
comes from the training split only, so the test split never leaks into
training. The split is stratified and driven by ``seed``, so the same
inputs always produce the same matrices.

Identifier columns (consecutive whole numbers, one per row, such as
``PassengerId``) are excluded automatically: they carry no information
about the target. User-facing messages are in Spanish.
"""

import csv
import math
import random
from collections import Counter
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_TRAIN_RATIO = 0.8
DEFAULT_SEED = 42
MAX_CLASSES = 20
MAX_CATEGORIES_PER_COLUMN = 50
MIN_ROWS_FOR_IDENTIFIER_CHECK = 10
MISSING_VALUES = frozenset({"", "na", "n/a", "nan", "null", "none", "?"})


class DatasetPreparationError(ValueError):
    """The dataset cannot be used to train the requested model."""


@dataclass
class NumericFeature:
    column: str
    mean: float
    std: float


@dataclass
class CategoricalFeature:
    column: str
    categories: list[str]


@dataclass
class PreparedDataset:
    feature_names: list[str]
    class_labels: list[str]
    x_train: list[list[float]]
    y_train: list[int]
    x_test: list[list[float]]
    y_test: list[int]
    numeric_features: list[NumericFeature] = field(default_factory=list)
    categorical_features: list[CategoricalFeature] = field(default_factory=list)
    target_column: str = ""
    input_columns: list[str] = field(default_factory=list)
    excluded_columns: list[str] = field(default_factory=list)

    def summary(self) -> dict[str, Any]:
        """JSON-friendly description of the preparation, without the data."""
        return {
            "feature_count": len(self.feature_names),
            "numeric_columns": [feature.column for feature in self.numeric_features],
            "categorical_columns": {
                feature.column: feature.categories
                for feature in self.categorical_features
            },
            "excluded_columns": self.excluded_columns,
            "class_labels": self.class_labels,
            "train_rows": len(self.y_train),
            "test_rows": len(self.y_test),
            "train_class_counts": _class_counts(self.y_train, self.class_labels),
            "test_class_counts": _class_counts(self.y_test, self.class_labels),
        }

    def preprocessing_spec(self) -> dict[str, Any]:
        """Everything needed to transform a new row exactly like training did."""
        return {
            "target_column": self.target_column,
            "input_columns": self.input_columns,
            "excluded_columns": self.excluded_columns,
            "numeric": [
                {"column": f.column, "mean": f.mean, "std": f.std}
                for f in self.numeric_features
            ],
            "categorical": [
                {"column": f.column, "categories": f.categories}
                for f in self.categorical_features
            ],
            "feature_names": self.feature_names,
            "class_labels": self.class_labels,
        }


def prepare_dataset(
    csv_path: Path,
    target_column: str,
    train_ratio: float = DEFAULT_TRAIN_RATIO,
    seed: int = DEFAULT_SEED,
) -> PreparedDataset:
    if not 0 < train_ratio < 1:
        raise DatasetPreparationError("La proporción de entrenamiento debe estar entre 0 y 1.")

    header, rows = _read_rows(csv_path)
    target_column = target_column.strip()
    if target_column not in header:
        raise DatasetPreparationError(
            f"La columna objetivo «{target_column}» no existe. "
            f"Columnas disponibles: {', '.join(header)}."
        )
    _reject_missing_values(header, rows)

    target_index = header.index(target_column)
    class_labels = _class_labels([row[target_index] for row in rows])
    label_index = {label: index for index, label in enumerate(class_labels)}
    labels = [label_index[row[target_index]] for row in rows]

    train_rows, test_rows = _stratified_split(labels, class_labels, train_ratio, seed)

    numeric: list[NumericFeature] = []
    categorical: list[CategoricalFeature] = []
    excluded: list[str] = []
    for index in (i for i in range(len(header)) if i != target_index):
        column = header[index]
        if _is_identifier_column(rows, index):
            excluded.append(column)
        elif _is_numeric_column(rows, index):
            values = [float(rows[row][index]) for row in train_rows]
            mean = sum(values) / len(values)
            variance = sum((value - mean) ** 2 for value in values) / len(values)
            std = math.sqrt(variance)
            numeric.append(NumericFeature(column, mean, std if std > 0 else 1.0))
        else:
            categories = sorted({rows[row][index] for row in train_rows})
            if len(categories) > MAX_CATEGORIES_PER_COLUMN:
                raise DatasetPreparationError(
                    f"La columna «{column}» tiene {len(categories)} valores de texto "
                    f"distintos (máximo {MAX_CATEGORIES_PER_COLUMN}). Parece un "
                    "identificador o texto libre; quítala del CSV."
                )
            categorical.append(CategoricalFeature(column, categories))

    if not numeric and not categorical:
        raise DatasetPreparationError(
            "No quedan columnas para entrenar: todas, salvo la objetivo, son identificadores."
        )

    encoders = _build_encoders(header, numeric, categorical)
    feature_names = [name for _, names, _ in encoders for name in names]

    def encode(row_indexes: list[int]) -> list[list[float]]:
        return [
            [value for index, _, encoder in encoders for value in encoder(rows[row][index])]
            for row in row_indexes
        ]

    return PreparedDataset(
        feature_names=feature_names,
        class_labels=class_labels,
        x_train=encode(train_rows),
        y_train=[labels[row] for row in train_rows],
        x_test=encode(test_rows),
        y_test=[labels[row] for row in test_rows],
        numeric_features=numeric,
        categorical_features=categorical,
        target_column=target_column,
        input_columns=[header[index] for index, _, _ in encoders],
        excluded_columns=excluded,
    )


def encode_row(spec: dict[str, Any], row: dict[str, str]) -> list[float]:
    """Encodes one new row with a model's preprocessing spec.

    Used to predict with a downloaded model: the same normalization and
    one-hot encoding as training, in the same column order.
    """
    numeric = {item["column"]: item for item in spec["numeric"]}
    categorical = {item["column"]: item["categories"] for item in spec["categorical"]}
    values: list[float] = []
    for column in spec["input_columns"]:
        raw = str(row.get(column, "")).strip()
        if column in numeric:
            number = _parse_float(raw)
            if number is None:
                raise DatasetPreparationError(f"La columna «{column}» debe ser numérica; llegó «{raw}».")
            values.append((number - numeric[column]["mean"]) / numeric[column]["std"])
        else:
            values.extend(1.0 if raw == category else 0.0 for category in categorical[column])
    return values


def _read_rows(csv_path: Path) -> tuple[list[str], list[list[str]]]:
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as csv_file:
            reader = csv.reader(csv_file, strict=True)
            header = [column.strip() for column in next(reader, [])]
            rows = [
                [value.strip() for value in row]
                for row in reader
                if any(value.strip() for value in row)
            ]
    except (OSError, UnicodeDecodeError, csv.Error) as exc:
        raise DatasetPreparationError(f"No se pudo leer el archivo del dataset ({exc}).") from exc

    if not header:
        raise DatasetPreparationError("El archivo del dataset no tiene encabezado.")
    if any(len(row) != len(header) for row in rows):
        raise DatasetPreparationError("Algunas filas no tienen tantos valores como el encabezado.")
    return header, rows


def _reject_missing_values(header: list[str], rows: list[list[str]]) -> None:
    missing = Counter(
        header[index]
        for row in rows
        for index, value in enumerate(row)
        if value.lower() in MISSING_VALUES
    )
    if missing:
        details = ", ".join(f"«{column}» ({count})" for column, count in missing.items())
        raise DatasetPreparationError(
            f"El dataset tiene valores faltantes en: {details}. "
            "Complétalos o quita esas filas antes de entrenar."
        )


def _class_labels(values: list[str]) -> list[str]:
    distinct = set(values)
    if len(distinct) < 2:
        raise DatasetPreparationError("La columna objetivo necesita al menos 2 clases.")
    if len(distinct) > MAX_CLASSES:
        raise DatasetPreparationError(
            f"La columna objetivo tiene {len(distinct)} valores distintos (máximo "
            f"{MAX_CLASSES}). Logistic Regression necesita un objetivo de categorías, "
            "no uno continuo."
        )

    counts = Counter(values)
    too_small = sorted(label for label, count in counts.items() if count < 2)
    if too_small:
        raise DatasetPreparationError(
            "Cada clase necesita al menos 2 filas para aparecer en train y en test; "
            f"faltan filas en: {', '.join(too_small)}."
        )

    if all(_parse_float(value) is not None for value in distinct):
        return sorted(distinct, key=float)
    return sorted(distinct)


def _stratified_split(
    labels: list[int],
    class_labels: list[str],
    train_ratio: float,
    seed: int,
) -> tuple[list[int], list[int]]:
    generator = random.Random(seed)
    train: list[int] = []
    test: list[int] = []
    for class_index in range(len(class_labels)):
        members = [row for row, label in enumerate(labels) if label == class_index]
        generator.shuffle(members)
        test_size = round(len(members) * (1 - train_ratio))
        test_size = min(max(test_size, 1), len(members) - 1)
        test.extend(members[:test_size])
        train.extend(members[test_size:])
    generator.shuffle(train)
    generator.shuffle(test)
    return train, test


def _is_identifier_column(rows: list[list[str]], index: int) -> bool:
    """Consecutive whole numbers, one distinct value per row (0..n-1, 1..n…)."""
    if len(rows) < MIN_ROWS_FOR_IDENTIFIER_CHECK:
        return False
    numbers = []
    for row in rows:
        number = _parse_float(row[index])
        if number is None or number != int(number):
            return False
        numbers.append(int(number))
    return len(set(numbers)) == len(numbers) and max(numbers) - min(numbers) == len(numbers) - 1


def _is_numeric_column(rows: list[list[str]], index: int) -> bool:
    return all(_parse_float(row[index]) is not None for row in rows)


def _parse_float(value: str) -> float | None:
    try:
        number = float(value)
    except ValueError:
        return None
    return number if math.isfinite(number) else None


def _build_encoders(
    header: list[str],
    numeric: list[NumericFeature],
    categorical: list[CategoricalFeature],
) -> list[tuple[int, list[str], Any]]:
    """Returns (column index, output names, encoder) in CSV column order."""
    encoders: dict[int, tuple[int, list[str], Any]] = {}
    for feature in numeric:
        mean, std = feature.mean, feature.std
        encoders[header.index(feature.column)] = (
            header.index(feature.column),
            [feature.column],
            lambda value, mean=mean, std=std: [(float(value) - mean) / std],
        )
    for feature in categorical:
        positions = {category: i for i, category in enumerate(feature.categories)}
        width = len(feature.categories)

        def one_hot(value: str, positions=positions, width=width) -> list[float]:
            # Categories never seen in training encode as all zeros.
            vector = [0.0] * width
            if value in positions:
                vector[positions[value]] = 1.0
            return vector

        encoders[header.index(feature.column)] = (
            header.index(feature.column),
            [f"{feature.column}={category}" for category in feature.categories],
            one_hot,
        )
    return [encoders[index] for index in sorted(encoders)]


def _class_counts(labels: list[int], class_labels: list[str]) -> dict[str, int]:
    counts = Counter(labels)
    return {label: counts.get(index, 0) for index, label in enumerate(class_labels)}
