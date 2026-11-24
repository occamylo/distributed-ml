import random
import unittest

from workers.training_engine import (
    Gradient,
    ModelParameters,
    aggregate_gradients,
    apply_update,
    compute_gradient,
    evaluate,
    initial_parameters,
    train_local,
    validate_hyperparameters,
)


def separable_data(rows: int, classes: int, seed: int = 1) -> tuple[list[list[float]], list[int]]:
    """Points around one center per class, easy to separate."""
    generator = random.Random(seed)
    centers = [[3.0 * c, -2.0 * c] for c in range(classes)]
    x, y = [], []
    for i in range(rows):
        label = i % classes
        x.append([centers[label][0] + generator.gauss(0, 0.5), centers[label][1] + generator.gauss(0, 0.5)])
        y.append(label)
    return x, y


def assert_matrices_close(test: unittest.TestCase, first, second, places: int = 10) -> None:
    for row_a, row_b in zip(first, second):
        for a, b in zip(row_a, row_b):
            test.assertAlmostEqual(a, b, places=places)


class TrainingEngineTests(unittest.TestCase):
    def test_initial_parameters_are_zero_with_the_right_shape(self) -> None:
        parameters = initial_parameters(feature_count=3, class_count=2)

        self.assertEqual(parameters.weights, [[0.0, 0.0]] * 3)
        self.assertEqual(parameters.bias, [0.0, 0.0])

    def test_weighted_partition_gradients_equal_the_full_batch_gradient(self) -> None:
        x, y = separable_data(rows=101, classes=3)
        parameters = apply_update(
            initial_parameters(2, 3), compute_gradient(initial_parameters(2, 3), x, y), 0.3
        )

        full = compute_gradient(parameters, x, y)
        partitions = [(0, 40), (40, 75), (75, 101)]
        aggregated = aggregate_gradients(
            [compute_gradient(parameters, x[a:b], y[a:b]) for a, b in partitions]
        )

        self.assertEqual(aggregated.sample_count, 101)
        assert_matrices_close(self, aggregated.weights, full.weights)
        assert_matrices_close(self, [aggregated.bias], [full.bias])
        self.assertAlmostEqual(aggregated.loss, full.loss, places=10)

    def test_update_moves_against_the_gradient(self) -> None:
        parameters = ModelParameters(weights=[[1.0, 2.0]], bias=[0.5, -0.5])
        gradient = Gradient(weights=[[0.2, -0.4]], bias=[1.0, 0.0], sample_count=10, loss=0.0)

        updated = apply_update(parameters, gradient, learning_rate=0.5)

        assert_matrices_close(self, updated.weights, [[0.9, 2.2]])
        assert_matrices_close(self, [updated.bias], [[0.0, -0.5]])

    def test_binary_training_reduces_loss_and_learns(self) -> None:
        x, y = separable_data(rows=200, classes=2)
        result = train_local(x[:160], y[:160], x[160:], y[160:], class_count=2)

        self.assertLess(result.loss_history[-1], result.loss_history[0])
        self.assertGreaterEqual(result.test_evaluation.accuracy, 0.95)

    def test_multiclass_training_learns_three_classes(self) -> None:
        x, y = separable_data(rows=300, classes=3)
        result = train_local(x[:240], y[:240], x[240:], y[240:], class_count=3)

        self.assertGreaterEqual(result.test_evaluation.accuracy, 0.95)
        self.assertEqual(len(result.test_evaluation.confusion_matrix), 3)

    def test_training_is_deterministic(self) -> None:
        x, y = separable_data(rows=100, classes=2)
        first = train_local(x[:80], y[:80], x[80:], y[80:], class_count=2, epochs=30)
        second = train_local(x[:80], y[:80], x[80:], y[80:], class_count=2, epochs=30)

        self.assertEqual(first.parameters, second.parameters)

    def test_evaluation_counts_the_confusion_matrix(self) -> None:
        parameters = ModelParameters(weights=[[1.0, -1.0]], bias=[0.0, 0.0])
        evaluation = evaluate(parameters, [[1.0], [2.0], [-1.0], [-3.0]], [0, 1, 1, 1])

        self.assertEqual(evaluation.confusion_matrix, [[1, 0], [1, 2]])
        self.assertEqual(evaluation.accuracy, 0.75)

    def test_parameters_and_gradients_round_trip_as_plain_data(self) -> None:
        x, y = separable_data(rows=20, classes=2)
        parameters = initial_parameters(2, 2)
        gradient = compute_gradient(parameters, x, y)

        self.assertEqual(ModelParameters.from_dict(parameters.to_dict()), parameters)
        self.assertEqual(Gradient.from_dict(gradient.to_dict()), gradient)

    def test_rejects_rows_with_the_wrong_number_of_features(self) -> None:
        with self.assertRaisesRegex(ValueError, "2 features"):
            compute_gradient(initial_parameters(2, 2), [[1.0, 2.0, 3.0]], [0])

    def test_rejects_labels_outside_the_model_classes(self) -> None:
        with self.assertRaisesRegex(ValueError, "class indexes"):
            compute_gradient(initial_parameters(1, 2), [[1.0]], [2])

    def test_rejects_invalid_hyperparameters(self) -> None:
        for learning_rate, epochs in [(0, 10), (-1, 10), (float("nan"), 10), (0.1, 0), (0.1, 2.5)]:
            with self.subTest(learning_rate=learning_rate, epochs=epochs):
                with self.assertRaises(ValueError):
                    validate_hyperparameters(learning_rate, epochs)


if __name__ == "__main__":
    unittest.main()
