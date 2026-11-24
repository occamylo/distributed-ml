import unittest
from collections import Counter
from unittest import mock
from urllib.error import HTTPError, URLError
from uuid import UUID, uuid4

from coordinator import dispatcher
from coordinator.recovery import MAX_TASK_ATTEMPTS, RecoveryError, plan_reassignments
from workers.training_engine import initial_parameters

W1, W2, W3, LEADER = (UUID(int=n) for n in (1, 2, 3, 9))


def job_tasks(owners: list[UUID], attempts: int = 1) -> list[dict]:
    return [
        {"task_id": uuid4(), "partition_index": i, "node_id": owner, "attempts": attempts}
        for i, owner in enumerate(owners)
    ]


class PlanReassignmentsTests(unittest.TestCase):
    def test_tasks_of_the_failed_worker_are_spread_over_the_survivors(self) -> None:
        tasks = job_tasks([W1, W2, W3] * 3)
        failed = [t for t in tasks if t["node_id"] == W2]

        plan = plan_reassignments(failed, tasks, [W1, W3], coordinator_node_id=None)

        self.assertEqual(len(plan), 3)
        self.assertNotIn(W2, {node for _, node in plan})
        self.assertEqual(Counter(node for _, node in plan), {W1: 2, W3: 1})

    def test_the_coordinator_goes_last(self) -> None:
        tasks = job_tasks([LEADER, W2, W3] * 2)
        failed = [t for t in tasks if t["node_id"] == W3]

        plan = plan_reassignments(failed, tasks, [LEADER, W2], coordinator_node_id=LEADER)

        # Both survivors hold 2 Tasks; the tie goes to the non-coordinator.
        self.assertEqual([node for _, node in plan], [W2, LEADER])

    def test_failed_worker_is_never_chosen_even_if_reported_alive(self) -> None:
        tasks = job_tasks([W1, W2])
        failed = [tasks[1]]

        plan = plan_reassignments(failed, tasks, [W2, W1], coordinator_node_id=None)

        self.assertEqual([node for _, node in plan], [W1])

    def test_other_online_workers_help_only_when_no_participant_survives(self) -> None:
        tasks = job_tasks([W1, W1])

        plan = plan_reassignments(tasks, tasks, [W3], coordinator_node_id=None)

        self.assertEqual({node for _, node in plan}, {W3})

    def test_exhausted_attempts_fail_the_recovery(self) -> None:
        tasks = job_tasks([W1, W2], attempts=MAX_TASK_ATTEMPTS)

        with self.assertRaisesRegex(RecoveryError, "agotó sus 3 intentos"):
            plan_reassignments([tasks[0]], tasks, [W2], coordinator_node_id=None)

    def test_no_survivors_fail_the_recovery(self) -> None:
        tasks = job_tasks([W1, W2])

        with self.assertRaisesRegex(RecoveryError, "No quedan Workers vivos"):
            plan_reassignments(tasks, tasks, [], coordinator_node_id=None)


class DispatchFailureTests(unittest.TestCase):
    def setUp(self) -> None:
        self.task = {"task_id": uuid4(), "node_id": W1, "partition_index": 0,
                     "attempt_id": uuid4(), "rows_assigned": 10}
        self.nodes = {W1: {"node_id": W1, "ip_address": "127.0.0.1", "hostname": "h"}}

    def dispatch_with(self, error: Exception) -> dispatcher.TaskFailure:
        with mock.patch.object(dispatcher, "urlopen", side_effect=error):
            result = dispatcher.dispatch_round(
                [self.task], self.nodes, initial_parameters(1, 2), leader_epoch=1, training_epoch=1
            )
        self.assertEqual(result.gradients, {})
        return result.failures[0]

    def test_unreachable_worker_is_retryable(self) -> None:
        self.assertTrue(self.dispatch_with(URLError(ConnectionRefusedError())).retryable)
        self.assertTrue(self.dispatch_with(ConnectionResetError()).retryable)
        self.assertTrue(self.dispatch_with(TimeoutError()).retryable)

    def test_server_errors_are_retryable_but_rejections_are_not(self) -> None:
        def http(code: int) -> HTTPError:
            return HTTPError("http://x", code, "error", {}, None)

        self.assertTrue(self.dispatch_with(http(503)).retryable)
        self.assertFalse(self.dispatch_with(http(409)).retryable)
        self.assertFalse(self.dispatch_with(http(422)).retryable)

    def test_task_without_known_worker_is_retryable(self) -> None:
        result = dispatcher.dispatch_round(
            [self.task], {}, initial_parameters(1, 2), leader_epoch=1, training_epoch=1
        )
        self.assertTrue(result.failures[0].retryable)


if __name__ == "__main__":
    unittest.main()
