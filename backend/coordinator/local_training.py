"""Local baseline: prepares a dataset and trains it in a single process.

Uses the same preparation and the same gradient operations as the
distributed run, so its metrics and duration are the reference for the
local vs distributed comparison.

Run from ``backend/``::

    python -m coordinator.local_training --csv ruta\\datos.csv --target y
    python -m coordinator.local_training --dataset-id <uuid> --target y
"""

import argparse
import json
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from uuid import UUID

from coordinator.aggregator import aggregate_gradients, apply_update
from coordinator.preprocessing import (
    DEFAULT_SEED,
    DEFAULT_TRAIN_RATIO,
    PreparedDataset,
    prepare_dataset,
)
from workers.training_engine import (
    DEFAULT_EPOCHS,
    DEFAULT_LEARNING_RATE,
    Evaluation,
    ModelParameters,
    compute_gradient,
    evaluate,
    initial_parameters,
    validate_hyperparameters,
)


@dataclass
class TrainingResult:
    parameters: ModelParameters
    train_evaluation: Evaluation
    test_evaluation: Evaluation
    duration_seconds: float
    epochs: int
    learning_rate: float
    loss_history: list[float] = field(default_factory=list)


def train_local(
    x_train: Sequence[Sequence[float]],
    y_train: Sequence[int],
    x_test: Sequence[Sequence[float]],
    y_test: Sequence[int],
    class_count: int,
    learning_rate: float = DEFAULT_LEARNING_RATE,
    epochs: int = DEFAULT_EPOCHS,
) -> TrainingResult:
    """Full-batch baseline on one process, with the distributed operations."""
    validate_hyperparameters(learning_rate, epochs)
    started = time.perf_counter()

    parameters = initial_parameters(len(x_train[0]), class_count)
    history: list[float] = []
    for _ in range(epochs):
        gradient = aggregate_gradients([compute_gradient(parameters, x_train, y_train)])
        history.append(gradient.loss)
        parameters = apply_update(parameters, gradient, learning_rate)

    return TrainingResult(
        parameters=parameters,
        train_evaluation=evaluate(parameters, x_train, y_train),
        test_evaluation=evaluate(parameters, x_test, y_test),
        duration_seconds=time.perf_counter() - started,
        epochs=epochs,
        learning_rate=learning_rate,
        loss_history=history,
    )


def run_local_baseline(
    csv_path: Path,
    target_column: str,
    train_ratio: float = DEFAULT_TRAIN_RATIO,
    seed: int = DEFAULT_SEED,
    learning_rate: float = DEFAULT_LEARNING_RATE,
    epochs: int = DEFAULT_EPOCHS,
) -> tuple[PreparedDataset, TrainingResult, float]:
    """Returns the prepared data, the training result and the total seconds."""
    started = time.perf_counter()
    prepared = prepare_dataset(csv_path, target_column, train_ratio, seed)
    result = train_local(
        prepared.x_train,
        prepared.y_train,
        prepared.x_test,
        prepared.y_test,
        class_count=len(prepared.class_labels),
        learning_rate=learning_rate,
        epochs=epochs,
    )
    return prepared, result, time.perf_counter() - started


def report(prepared: PreparedDataset, result: TrainingResult, total_seconds: float) -> dict[str, Any]:
    history = result.loss_history
    checkpoints = sorted({0, len(history) // 4, len(history) // 2, 3 * len(history) // 4, len(history) - 1})
    return {
        "preparation": prepared.summary(),
        "learning_rate": result.learning_rate,
        "epochs": result.epochs,
        "train_loss_by_epoch": {str(epoch + 1): round(history[epoch], 6) for epoch in checkpoints},
        "train": result.train_evaluation.to_dict(),
        "test": result.test_evaluation.to_dict(prepared.class_labels),
        "training_seconds": round(result.duration_seconds, 4),
        "total_seconds": round(total_seconds, 4),
    }


def _dataset_path(dataset_id: UUID) -> Path:
    # Imported here so --csv runs work without a database connection.
    import dataset_storage
    from database.connection import close_pool, initialize_pool
    from database.repository import get_dataset

    initialize_pool()
    try:
        dataset = get_dataset(dataset_id)
    finally:
        close_pool()
    if dataset is None:
        raise SystemExit(f"Dataset {dataset_id} not found.")
    return dataset_storage.resolve_path(dataset["file_path"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Train the local Logistic Regression baseline.")
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--csv", type=Path, help="Path to a CSV file.")
    source.add_argument("--dataset-id", type=UUID, help="Dataset registered with POST /datasets.")
    parser.add_argument("--target", required=True, help="Target column.")
    parser.add_argument("--train-ratio", type=float, default=DEFAULT_TRAIN_RATIO)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--learning-rate", type=float, default=DEFAULT_LEARNING_RATE)
    parser.add_argument("--epochs", type=int, default=DEFAULT_EPOCHS)
    args = parser.parse_args()

    csv_path = args.csv if args.csv else _dataset_path(args.dataset_id)
    prepared, result, total = run_local_baseline(
        csv_path,
        args.target,
        args.train_ratio,
        args.seed,
        args.learning_rate,
        args.epochs,
    )
    output = json.dumps(report(prepared, result, total), indent=2, ensure_ascii=False)
    sys.stdout.buffer.write((output + "\n").encode("utf-8"))


if __name__ == "__main__":
    main()
