"""The downloadable model predicts exactly like the trained model."""

import csv
import importlib.util
import json
import random
import tempfile
import unittest
from pathlib import Path

from coordinator.local_training import train_local
from coordinator.model_export import MODEL_FORMAT, build_model_artifact, predict_row
from coordinator.preprocessing import prepare_dataset

EXAMPLE = Path(__file__).resolve().parents[2] / "examples" / "predecir.py"


def load_example():
    spec = importlib.util.spec_from_file_location("predecir_example", EXAMPLE)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ModelExportTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls._tmp = tempfile.TemporaryDirectory()
        cls.csv_path = Path(cls._tmp.name) / "datos.csv"
        generator = random.Random(5)
        rows = []
        for i in range(150):
            ciudad = ["Bogotá", "Cali", "Medellín"][i % 3]
            horas = generator.uniform(0, 10)
            label = "sí" if horas + (2 if ciudad == "Cali" else 0) + generator.gauss(0, 1) > 6 else "no"
            rows.append({"id": i, "horas": f"{horas:.2f}", "ciudad": ciudad, "aprobo": label})
        with cls.csv_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "horas", "ciudad", "aprobo"])
            writer.writeheader()
            writer.writerows(rows)
        cls.rows = [{k: str(v) for k, v in row.items()} for row in rows]

        cls.prepared = prepare_dataset(cls.csv_path, "aprobo")
        result = train_local(
            cls.prepared.x_train, cls.prepared.y_train,
            cls.prepared.x_test, cls.prepared.y_test,
            class_count=len(cls.prepared.class_labels), epochs=150,
        )
        cls.result = result
        cls.artifact = build_model_artifact(
            cls.prepared, result.parameters, algorithm="logistic-regression",
            learning_rate=0.5, epochs=150, test_accuracy=result.test_evaluation.accuracy,
        )

    @classmethod
    def tearDownClass(cls) -> None:
        cls._tmp.cleanup()

    def test_artifact_is_plain_json_with_the_preprocessing(self) -> None:
        artifact = json.loads(json.dumps(self.artifact))

        self.assertEqual(artifact["format"], MODEL_FORMAT)
        self.assertEqual(artifact["target_column"], "aprobo")
        self.assertEqual(artifact["excluded_columns"], ["id"])
        self.assertEqual(artifact["input_columns"], ["horas", "ciudad"])
        self.assertEqual(len(artifact["weights"]), len(artifact["feature_names"]))

    def test_predictions_match_the_trained_model_on_every_row(self) -> None:
        # The artifact's accuracy over all rows must equal the engine's
        # train and test evaluations combined.
        r = self.result
        expected = (
            r.train_evaluation.accuracy * r.train_evaluation.sample_count
            + r.test_evaluation.accuracy * r.test_evaluation.sample_count
        )
        hits = sum(predict_row(self.artifact, row)[0] == row["aprobo"] for row in self.rows)

        self.assertEqual(hits, round(expected))

    def test_standalone_example_predicts_the_same_as_the_backend(self) -> None:
        example = load_example()
        model_path = Path(self._tmp.name) / "modelo.json"
        model_path.write_text(json.dumps(self.artifact), encoding="utf-8")
        model = example.cargar_modelo(model_path)

        for row in self.rows[:30]:
            label, probabilities = example.predecir_fila(model, row)
            backend_label, backend_probabilities = predict_row(self.artifact, row)
            self.assertEqual(label, backend_label)
            for key in probabilities:
                self.assertAlmostEqual(probabilities[key], backend_probabilities[key], places=12)

    def test_standalone_example_writes_a_predictions_csv(self) -> None:
        example = load_example()
        output = Path(self._tmp.name) / "predicciones.csv"
        total = example.predecir_csv(self.artifact, self.csv_path, output)

        with output.open(encoding="utf-8") as f:
            written = list(csv.DictReader(f))
        self.assertEqual(total, 150)
        self.assertIn("prediccion", written[0])
        self.assertIn("prob_sí", written[0])


if __name__ == "__main__":
    unittest.main()
