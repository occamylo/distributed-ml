"""Logistic Regression with softmax, written for data parallelism.

This module holds the model and what a Worker computes:

* ``compute_gradient``: the *mean* gradient of one partition with the
  current global weights, plus its number of rows, so the Coordinator can
  weight it.
* ``evaluate``: loss, accuracy and confusion matrix of some rows.

The Coordinator's side (weighted aggregation and the weight update) lives in
``coordinator/aggregator.py``, and the single-process baseline in
``coordinator/local_training.py``.

Softmax covers both binary and multiclass targets with one formula.
Parameters travel as plain lists so they can be sent over HTTP or stored
as JSON.
"""

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import torch
import torch.nn.functional as F

DTYPE = torch.float64
DEFAULT_LEARNING_RATE = 0.5
DEFAULT_EPOCHS = 100
MAX_EPOCHS = 10_000


@dataclass
class ModelParameters:
    """Weights (features x classes) and bias (classes)."""

    weights: list[list[float]]
    bias: list[float]

    @property
    def feature_count(self) -> int:
        return len(self.weights)

    @property
    def class_count(self) -> int:
        return len(self.bias)

    def to_dict(self) -> dict[str, Any]:
        return {"weights": self.weights, "bias": self.bias}

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ModelParameters":
        return cls(
            weights=[[float(value) for value in row] for row in data["weights"]],
            bias=[float(value) for value in data["bias"]],
        )


@dataclass
class Gradient:
    """Mean gradient of one partition plus the rows it came from."""

    weights: list[list[float]]
    bias: list[float]
    sample_count: int
    loss: float

    def to_dict(self) -> dict[str, Any]:
        return {
            "weights": self.weights,
            "bias": self.bias,
            "sample_count": self.sample_count,
            "loss": self.loss,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Gradient":
        return cls(
            weights=[[float(value) for value in row] for row in data["weights"]],
            bias=[float(value) for value in data["bias"]],
            sample_count=int(data["sample_count"]),
            loss=float(data["loss"]),
        )


@dataclass
class Evaluation:
    loss: float
    accuracy: float
    sample_count: int
    confusion_matrix: list[list[int]]

    def to_dict(self, class_labels: Sequence[str] | None = None) -> dict[str, Any]:
        data: dict[str, Any] = {
            "loss": self.loss,
            "accuracy": self.accuracy,
            "sample_count": self.sample_count,
            "confusion_matrix": self.confusion_matrix,
        }
        if class_labels is not None:
            data["class_labels"] = list(class_labels)
        return data


def initial_parameters(feature_count: int, class_count: int) -> ModelParameters:
    """Zero weights: deterministic, so every run starts from the same point."""
    if feature_count < 1 or class_count < 2:
        raise ValueError("The model needs at least 1 feature and 2 classes.")
    return ModelParameters(
        weights=[[0.0] * class_count for _ in range(feature_count)],
        bias=[0.0] * class_count,
    )


def compute_gradient(
    parameters: ModelParameters,
    x: Sequence[Sequence[float]],
    y: Sequence[int],
) -> Gradient:
    """Mean cross-entropy gradient of one partition, via autograd."""
    if len(x) == 0 or len(x) != len(y):
        raise ValueError("A partition needs the same, non-zero number of rows and labels.")

    weights, bias = _tensors(parameters, requires_grad=True)
    features, labels = _data(x, y, parameters)
    loss = F.cross_entropy(features @ weights + bias, labels)
    loss.backward()
    return Gradient(
        weights=weights.grad.tolist(),
        bias=bias.grad.tolist(),
        sample_count=len(y),
        loss=float(loss.item()),
    )


def evaluate(
    parameters: ModelParameters,
    x: Sequence[Sequence[float]],
    y: Sequence[int],
) -> Evaluation:
    if len(x) == 0 or len(x) != len(y):
        raise ValueError("Evaluation needs the same, non-zero number of rows and labels.")

    weights, bias = _tensors(parameters)
    features, labels = _data(x, y, parameters)
    with torch.no_grad():
        logits = features @ weights + bias
        loss = float(F.cross_entropy(logits, labels).item())
        predictions = logits.argmax(dim=1)

    classes = parameters.class_count
    confusion = [[0] * classes for _ in range(classes)]
    for actual, predicted in zip(labels.tolist(), predictions.tolist()):
        confusion[actual][predicted] += 1
    correct = sum(confusion[index][index] for index in range(classes))
    return Evaluation(loss, correct / len(y), len(y), confusion)


def validate_hyperparameters(learning_rate: float, epochs: int) -> None:
    if not math.isfinite(learning_rate) or learning_rate <= 0:
        raise ValueError("learning_rate debe ser un número positivo.")
    if isinstance(epochs, bool) or not isinstance(epochs, int) or not 1 <= epochs <= MAX_EPOCHS:
        raise ValueError(f"epochs debe ser un entero entre 1 y {MAX_EPOCHS}.")


def _tensors(
    parameters: ModelParameters,
    requires_grad: bool = False,
) -> tuple[torch.Tensor, torch.Tensor]:
    weights = torch.tensor(parameters.weights, dtype=DTYPE, requires_grad=requires_grad)
    bias = torch.tensor(parameters.bias, dtype=DTYPE, requires_grad=requires_grad)
    return weights, bias


def _data(
    x: Sequence[Sequence[float]],
    y: Sequence[int],
    parameters: ModelParameters,
) -> tuple[torch.Tensor, torch.Tensor]:
    features = torch.tensor(x, dtype=DTYPE)
    if features.dim() != 2 or features.shape[1] != parameters.feature_count:
        raise ValueError(
            f"Expected rows with {parameters.feature_count} features; "
            f"got shape {tuple(features.shape)}."
        )
    labels = torch.tensor(y, dtype=torch.long)
    if labels.min() < 0 or labels.max() >= parameters.class_count:
        raise ValueError("Labels must be class indexes of the model.")
    return features, labels
