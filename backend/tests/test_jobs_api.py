import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi import HTTPException
from pydantic import ValidationError

from api.jobs import JobCreate, create_job
from coordinator.preprocessing import DatasetPreparationError
from database.repository import create_job as create_job_in_repository


def existing_file() -> MagicMock:
    path = MagicMock(spec=Path)
    path.is_file.return_value = True
    return path


class JobCreationTests(unittest.TestCase):
    def test_api_creates_job_without_requested_worker_count(self) -> None:
        dataset_id = uuid4()
        job_id = uuid4()
        request = JobCreate(
            dataset_id=dataset_id,
            algorithm="logistic-regression",
            target_column="label",
        )
        prepared = SimpleNamespace(
            summary=lambda: {"train_rows": 8, "test_rows": 2}, input_columns=["feature"]
        )
        with (
            patch("api.jobs.repository.get_dataset", return_value={"file_path": "storage/datasets/x.csv"}),
            patch("api.jobs.dataset_storage.resolve_path", return_value=existing_file()),
            patch("api.jobs.dataset_storage.read_columns", return_value=["feature", "label"]),
            patch("api.jobs.prepare_dataset", return_value=prepared),
            patch("api.jobs.repository.create_job", return_value={"job_id": job_id}) as repository_create_job,
        ):
            result = create_job(request)

        self.assertEqual(result["job_id"], job_id)
        self.assertEqual(result["preparation"], {"train_rows": 8, "test_rows": 2})
        repository_create_job.assert_called_once_with(
            dataset_id=dataset_id,
            algorithm="logistic-regression",
            target_column="label",
            status="pending",
            parameters={
                "learning_rate": 0.5,
                "epochs": 100,
                "dataset_headers": ["feature", "label"],
                "feature_columns": ["feature"],
            },
            train_ratio=0.8,
            seed=42,
        )

    def test_a_legacy_worker_count_in_the_request_is_ignored(self) -> None:
        request = JobCreate(dataset_id=uuid4(), target_column="label", requested_workers=3)
        self.assertNotIn("requested_workers", request.model_dump())

    def test_only_logistic_regression_is_accepted(self) -> None:
        with self.assertRaises(ValidationError):
            JobCreate(dataset_id=uuid4(), algorithm="linear-regression", target_column="y")

    def test_unusable_dataset_is_rejected_before_creating_the_job(self) -> None:
        request = JobCreate(dataset_id=uuid4(), target_column="label")
        with (
            patch("api.jobs.repository.get_dataset", return_value={"file_path": "storage/datasets/x.csv"}),
            patch("api.jobs.dataset_storage.resolve_path", return_value=existing_file()),
            patch("api.jobs.prepare_dataset", side_effect=DatasetPreparationError("sin clases")),
            patch("api.jobs.repository.create_job") as repository_create_job,
        ):
            with self.assertRaises(HTTPException) as raised:
                create_job(request)

        self.assertEqual(raised.exception.status_code, 422)
        repository_create_job.assert_not_called()

    def test_repository_insert_does_not_set_legacy_worker_count(self) -> None:
        dataset_id = uuid4()
        with patch(
            "database.repository._insert",
            return_value={"dataset_id": dataset_id},
        ) as insert:
            create_job_in_repository(
                dataset_id=dataset_id,
                algorithm="logistic-regression",
                target_column="label",
            )

        table, values = insert.call_args.args
        self.assertEqual(table, "jobs")
        self.assertNotIn("requested_workers", values)
        self.assertEqual(values["status"], "pending")


if __name__ == "__main__":
    unittest.main()
