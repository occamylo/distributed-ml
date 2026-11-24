import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from api.jobs import JobCreate, create_job
from database.repository import create_job as create_job_in_repository


class JobCreationTests(unittest.TestCase):
    def test_api_creates_job_without_requested_worker_count(self) -> None:
        dataset_id = uuid4()
        job_id = uuid4()
        request = JobCreate(
            dataset_id=dataset_id,
            algorithm="logistic-regression",
            target_column="label",
        )
        with (
            patch(
                "api.jobs.repository.get_dataset",
                return_value={"file_path": "dataset.csv"},
            ),
            patch(
                "api.jobs.resolve_uploaded_dataset_path",
                return_value=Path("dataset.csv"),
            ),
            patch(
                "api.jobs.inspect_csv",
                return_value=SimpleNamespace(headers=("feature", "label")),
            ),
            patch(
                "api.jobs.repository.create_job",
                return_value={"job_id": job_id},
            ) as repository_create_job,
        ):
            result = create_job(request)

        self.assertEqual(result["job_id"], job_id)
        repository_create_job.assert_called_once_with(
            dataset_id=dataset_id,
            algorithm="logistic-regression",
            target_column="label",
            status="pending",
            parameters={
                "dataset_headers": ["feature", "label"],
                "feature_columns": ["feature"],
            },
            train_ratio=None,
            seed=None,
        )

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
