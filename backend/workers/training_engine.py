"""Logistic Regression with softmax, written for data parallelism.

The same three operations serve the local baseline and the distributed run:

* ``compute_gradient``: what a Worker does with its partition and the
  current global weights. It returns the *mean* gradient of the partition
  and the number of rows, so the Coordinator can weight it.
* ``aggregate_gradients``: the Coordinator's weighted average
  G = sum(n_i * G_i) / sum(n_i).
* ``apply_update``: the Coordinator's step w <- w - learning_rate * G.

Because the weighted average of per-partition mean gradients equals the
full-batch gradient, a distributed epoch and a local epoch produce the same
weights (up to floating point rounding).

Softmax covers both binary and multiclass targets with one formula.
Parameters travel as plain lists so they can be sent over HTTP or stored
as JSON.
"""

import math
import time
from collections.abc import Sequence
from dataclasses import dataclass, field
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


@dataclass
class TrainingResult:
    parameters: ModelParameters
    train_evaluation: Evaluation
    test_evaluation: Evaluation
    duration_seconds: float
    epochs: int
    learning_rate: float
    loss_history: list[float] = field(default_factory=list)


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


def aggregate_gradients(gradients: Sequence[Gradient]) -> Gradient:
    """Weighted average by sample count: sum(n_i * G_i) / sum(n_i)."""
    if not gradients:
        raise ValueError("At least one gradient is required.")
    total = sum(gradient.sample_count for gradient in gradients)
    if total <= 0:
        raise ValueError("Gradients must come from at least one row.")

    weights = sum(
        torch.tensor(g.weights, dtype=DTYPE) * g.sample_count for g in gradients
    ) / total
    bias = sum(torch.tensor(g.bias, dtype=DTYPE) * g.sample_count for g in gradients) / total
    loss = sum(g.loss * g.sample_count for g in gradients) / total
    return Gradient(weights.tolist(), bias.tolist(), total, loss)


def apply_update(
    parameters: ModelParameters,
    gradient: Gradient,
    learning_rate: float,
) -> ModelParameters:
    """One gradient-descent step: w <- w - learning_rate * G."""
    weights, bias = _tensors(parameters)
    new_weights = weights - learning_rate * torch.tensor(gradient.weights, dtype=DTYPE)
    new_bias = bias - learning_rate * torch.tensor(gradient.bias, dtype=DTYPE)
    return ModelParameters(new_weights.tolist(), new_bias.tolist())


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


def validate_hyperparameters(learning_rate: float, epochs: int) -> None:
    if not math.isfinite(learning_rate) or learning_rate <= 0:
        raise ValueError("learning_rate must be a positive number.")
    if isinstance(epochs, bool) or not isinstance(epochs, int) or not 1 <= epochs <= MAX_EPOCHS:
        raise ValueError(f"epochs must be an integer between 1 and {MAX_EPOCHS}.")


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
