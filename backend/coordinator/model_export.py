"""The trained model as a file the user can take to their own project.

The artifact is plain JSON: the preprocessing learned on the training split
(input columns, means and standard deviations, one-hot categories), the class
labels and the final weights. ``examples/predecir.py`` uses it with the Python
standard library alone, so the user's project needs neither PyTorch nor this
repository.
"""

import math
from datetime import datetime, timezone
from typing import Any

from coordinator.preprocessing import PreparedDataset, encode_row
from workers.training_engine import ModelParameters

MODEL_FORMAT = "distributed-ml/logistic-regression@1"


def build_model_artifact(
    prepared: PreparedDataset,
    weights: ModelParameters,
    *,
    algorithm: str,
    learning_rate: float,
    epochs: int,
    test_accuracy: float,
) -> dict[str, Any]:
    return {
        "format": MODEL_FORMAT,
        "algorithm": algorithm,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "training": {
            "learning_rate": learning_rate,
            "epochs": epochs,
            "test_accuracy": test_accuracy,
            "train_rows": len(prepared.y_train),
            "test_rows": len(prepared.y_test),
        },
        **prepared.preprocessing_spec(),
        "weights": weights.weights,
        "bias": weights.bias,
    }


def predict_row(artifact: dict[str, Any], row: dict[str, str]) -> tuple[str, dict[str, float]]:
    """Class label and probability per class for one raw CSV row."""
    features = encode_row(artifact, row)
    logits = [
        artifact["bias"][k] + sum(x * artifact["weights"][i][k] for i, x in enumerate(features))
        for k in range(len(artifact["class_labels"]))
    ]
    top = max(logits)
    exps = [math.exp(value - top) for value in logits]
    total = sum(exps)
    probabilities = {
        label: value / total for label, value in zip(artifact["class_labels"], exps)
    }
    return max(probabilities, key=probabilities.get), probabilities
