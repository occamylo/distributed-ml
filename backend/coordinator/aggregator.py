"""Coordinator side of data-parallel training.

* ``aggregate_gradients``: weighted average G = sum(n_i * G_i) / sum(n_i)
  of the partition gradients returned by the Workers.
* ``apply_update``: one gradient-descent step w <- w - learning_rate * G.

Because the weighted average of per-partition mean gradients equals the
full-batch gradient, a distributed epoch and a local epoch produce the same
weights (up to floating point rounding).
"""

from collections.abc import Sequence

import torch

from workers.training_engine import DTYPE, Gradient, ModelParameters


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
    weights = torch.tensor(parameters.weights, dtype=DTYPE)
    bias = torch.tensor(parameters.bias, dtype=DTYPE)
    new_weights = weights - learning_rate * torch.tensor(gradient.weights, dtype=DTYPE)
    new_bias = bias - learning_rate * torch.tensor(gradient.bias, dtype=DTYPE)
    return ModelParameters(new_weights.tolist(), new_bias.tolist())
