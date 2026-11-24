import unittest
from types import SimpleNamespace
from unittest.mock import patch
from uuid import uuid4

from coordinator.scheduler import Scheduler


class SchedulerWorkerSelectionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.scheduler = Scheduler(SimpleNamespace(node_id=uuid4()))

    def test_eligible_worker_selection_returns_every_eligible_worker(self) -> None:
        workers = [
            {
                "node_id": uuid4(),
                "status": "online",
                "cpu_available_cores": 2.0,
                "ram_available_mb": 2048,
                "active_tasks": 0,
            }
            for _ in range(3)
        ]
        nodes = [
            *workers,
            {
                "node_id": uuid4(),
                "status": "offline",
                "cpu_available_cores": 8.0,
                "ram_available_mb": 8192,
                "active_tasks": 0,
            },
        ]

        with patch(
            "coordinator.scheduler.list_nodes_with_fresh_metrics",
            return_value=nodes,
        ):
            selected = self.scheduler._eligible_workers()

        self.assertEqual({worker["node_id"] for worker in selected},
                         {worker["node_id"] for worker in workers})

    def test_scheduler_ignores_legacy_count_and_assigns_to_all_workers(self) -> None:
        job_id = uuid4()
        dataset_id = uuid4()
        workers = [{"node_id": uuid4()} for _ in range(3)]
        tasks = [
            {"task_id": uuid4(), "node_id": None, "status": "pending"}
            for _ in workers
        ]
        assigned_tasks = [
            {
                "task_id": task["task_id"],
                "node_id": worker["node_id"],
                "status": "assigned",
            }
            for task, worker in zip(tasks, workers)
        ]

        with (
            patch.object(self.scheduler, "_eligible_workers", return_value=workers),
            patch.object(self.scheduler, "_retains_leadership", return_value=True),
            patch("coordinator.scheduler.get_dataset", return_value={"row_count": 10}),
            patch(
                "coordinator.scheduler.list_tasks_by_job",
                side_effect=[[], assigned_tasks],
            ),
            patch(
                "coordinator.scheduler.create_tasks_for_job",
                return_value=tasks,
            ) as create_tasks,
            patch(
                "coordinator.scheduler.assign_task",
                side_effect=[{"task_id": task["task_id"]} for task in tasks],
            ) as assign_task,
            patch(
                "coordinator.scheduler.update_job_as_leader",
                return_value={"job_id": job_id},
            ) as update_job,
        ):
            completed = self.scheduler._schedule_job(
                {
                    "job_id": job_id,
                    "dataset_id": dataset_id,
                    "status": "pending",
                    "requested_workers": 1,
                },
                epoch=1,
            )

        self.assertTrue(completed)
        partitions = create_tasks.call_args.args[1]
        self.assertEqual(len(partitions), len(workers))
        self.assertEqual(
            {partition["rows_assigned"] for partition in partitions},
            {3, 4},
        )
        self.assertEqual(assign_task.call_count, len(workers))
        self.assertEqual(
            {item.args[1] for item in assign_task.call_args_list},
            {worker["node_id"] for worker in workers},
        )
        self.assertEqual(update_job.call_args.args[2]["actual_workers"], len(workers))

    def test_job_stays_pending_without_eligible_workers(self) -> None:
        with (
            patch.object(self.scheduler, "_eligible_workers", return_value=[]),
            patch("coordinator.scheduler.get_dataset") as get_dataset,
            patch("coordinator.scheduler.list_tasks_by_job") as list_tasks,
            patch("coordinator.scheduler.create_tasks_for_job") as create_tasks,
        ):
            completed = self.scheduler._schedule_job(
                {"job_id": uuid4(), "dataset_id": uuid4(), "status": "pending"},
                epoch=1,
            )

        self.assertTrue(completed)
        get_dataset.assert_not_called()
        list_tasks.assert_not_called()
        create_tasks.assert_not_called()


if __name__ == "__main__":
    unittest.main()
