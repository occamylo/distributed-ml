import math
import tempfile
import unittest
from pathlib import Path

from coordinator.preprocessing import DatasetPreparationError, encode_row, prepare_dataset


class PrepareDatasetTests(unittest.TestCase):
    def setUp(self) -> None:
        self._tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self._tmp.cleanup)

    def write(self, content: str) -> Path:
        path = Path(self._tmp.name) / "data.csv"
        path.write_text(content, encoding="utf-8")
        return path

    def binary_csv(self, rows: int = 40) -> Path:
        lines = ["edad,ciudad,aprobó"]
        for i in range(rows):
            lines.append(f"{18 + (i * 7) % 45},{['Bogotá', 'Cali', 'Medellín'][i % 3]},{'sí' if i % 2 else 'no'}")
        return self.write("\n".join(lines) + "\n")

    def test_encodes_numeric_and_categorical_features_in_column_order(self) -> None:
        prepared = prepare_dataset(self.binary_csv(), "aprobó")

        self.assertEqual(
            prepared.feature_names,
            ["edad", "ciudad=Bogotá", "ciudad=Cali", "ciudad=Medellín"],
        )
        self.assertEqual(prepared.class_labels, ["no", "sí"])
        for row in prepared.x_train + prepared.x_test:
            self.assertEqual(len(row), 4)
            self.assertEqual(sum(row[1:]), 1.0)

    def test_split_is_stratified_and_respects_train_ratio(self) -> None:
        prepared = prepare_dataset(self.binary_csv(rows=100), "aprobó", train_ratio=0.8)

        self.assertEqual(len(prepared.y_train), 80)
        self.assertEqual(len(prepared.y_test), 20)
        self.assertEqual(prepared.y_test.count(0), 10)
        self.assertEqual(prepared.y_test.count(1), 10)

    def test_same_seed_gives_the_same_split(self) -> None:
        first = prepare_dataset(self.binary_csv(), "aprobó", seed=7)
        second = prepare_dataset(self.binary_csv(), "aprobó", seed=7)
        other = prepare_dataset(self.binary_csv(), "aprobó", seed=8)

        self.assertEqual(first.x_train, second.x_train)
        self.assertNotEqual(first.x_train, other.x_train)

    def test_normalization_uses_training_statistics_only(self) -> None:
        prepared = prepare_dataset(self.binary_csv(rows=60), "aprobó")
        train_ages = [row[0] for row in prepared.x_train]

        self.assertAlmostEqual(sum(train_ages) / len(train_ages), 0.0, places=9)
        variance = sum(value**2 for value in train_ages) / len(train_ages)
        self.assertAlmostEqual(math.sqrt(variance), 1.0, places=9)

    def test_multiclass_numeric_target_is_sorted_numerically(self) -> None:
        lines = ["x,clase"] + [f"{i * 3},{[10, 2, 1][i % 3]}" for i in range(30)]
        prepared = prepare_dataset(self.write("\n".join(lines)), "clase")

        self.assertEqual(prepared.class_labels, ["1", "2", "10"])
        self.assertEqual(set(prepared.y_test), {0, 1, 2})

    def test_constant_column_does_not_divide_by_zero(self) -> None:
        lines = ["fijo,y"] + [f"5,{i % 2}" for i in range(10)]
        prepared = prepare_dataset(self.write("\n".join(lines)), "y")

        self.assertTrue(all(row == [0.0] for row in prepared.x_train))

    def test_rejects_unknown_target(self) -> None:
        with self.assertRaisesRegex(DatasetPreparationError, "no existe"):
            prepare_dataset(self.binary_csv(), "resultado")

    def test_rejects_missing_values_and_names_the_column(self) -> None:
        path = self.write("a,b,y\n1,,0\n2,3,1\n3,NA,0\n4,5,1\n")
        with self.assertRaisesRegex(DatasetPreparationError, r"«b» \(2\)"):
            prepare_dataset(path, "y")

    def test_rejects_single_class_target(self) -> None:
        path = self.write("a,y\n1,0\n2,0\n3,0\n")
        with self.assertRaisesRegex(DatasetPreparationError, "al menos 2 clases"):
            prepare_dataset(path, "y")

    def test_rejects_continuous_target(self) -> None:
        lines = ["a,precio"] + [f"{i},{i * 1.5}" for i in range(40)]
        with self.assertRaisesRegex(DatasetPreparationError, "continuo"):
            prepare_dataset(self.write("\n".join(lines)), "precio")

    def test_rejects_class_with_a_single_row(self) -> None:
        path = self.write("a,y\n1,0\n2,0\n3,1\n4,1\n5,2\n")
        with self.assertRaisesRegex(DatasetPreparationError, "faltan filas en: 2"):
            prepare_dataset(path, "y")

    def test_rejects_identifier_like_text_column(self) -> None:
        lines = ["codigo,y"] + [f"id-{i},{i % 2}" for i in range(120)]
        with self.assertRaisesRegex(DatasetPreparationError, "«codigo»"):
            prepare_dataset(self.write("\n".join(lines)), "y")

    def test_identifier_columns_are_excluded_automatically(self) -> None:
        lines = ["PassengerId,NumerPerson,edad,y"] + [
            f"{i + 1},{i},{18 + (i * 7) % 45},{i % 2}" for i in range(40)
        ]
        prepared = prepare_dataset(self.write(chr(10).join(lines)), "y")

        self.assertEqual(prepared.excluded_columns, ["PassengerId", "NumerPerson"])
        self.assertEqual(prepared.input_columns, ["edad"])
        self.assertEqual(prepared.feature_names, ["edad"])
        self.assertEqual(prepared.summary()["excluded_columns"], ["PassengerId", "NumerPerson"])

    def test_small_or_repeated_whole_numbers_are_not_identifiers(self) -> None:
        lines = ["cantidad,y"] + [f"{i % 5},{i % 2}" for i in range(40)]
        prepared = prepare_dataset(self.write(chr(10).join(lines)), "y")

        self.assertEqual(prepared.excluded_columns, [])

    def test_only_identifiers_left_is_rejected(self) -> None:
        lines = ["id,y"] + [f"{i},{i % 2}" for i in range(20)]
        with self.assertRaisesRegex(DatasetPreparationError, "identificadores"):
            prepare_dataset(self.write(chr(10).join(lines)), "y")

    def test_encode_row_reproduces_the_training_encoding(self) -> None:
        prepared = prepare_dataset(self.binary_csv(rows=60), "aprobó")
        spec = prepared.preprocessing_spec()
        numeric = prepared.numeric_features[0]

        encoded = encode_row(spec, {"edad": "30", "ciudad": "Cali", "aprobó": "sí"})

        self.assertAlmostEqual(encoded[0], (30 - numeric.mean) / numeric.std)
        self.assertEqual(encoded[1:], [0.0, 1.0, 0.0])


if __name__ == "__main__":
    unittest.main()
