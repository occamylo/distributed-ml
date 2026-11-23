import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from database.connection import close_pool, get_pool, initialize_pool
from database.repository import (
    DEFAULT_CLUSTER_ID,
    acquire_leadership,
    get_leadership,
    initialize_leadership,
    release_leadership,
    renew_leadership,
    validate_leadership,
)


class LeadershipRepositoryIntegrationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        initialize_pool()
        with get_pool().connection() as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT node_id FROM nodes ORDER BY node_id LIMIT 2")
                node_ids = [row[0] for row in cursor.fetchall()]
        if len(node_ids) < 2:
            close_pool()
            raise unittest.SkipTest(
                "Leadership integration tests require two registered nodes."
            )
        cls.node_a, cls.node_b = node_ids
        state = initialize_leadership(DEFAULT_CLUSTER_ID)
        if state["is_valid"]:
            close_pool()
            raise unittest.SkipTest(
                "The default leadership lease is active; run tests without a leader."
            )
        if state["leader_node_id"] is not None:
            release_leadership(state["leader_node_id"], state["epoch"])

    @classmethod
    def tearDownClass(cls) -> None:
        close_pool()

    def setUp(self) -> None:
        state = get_leadership(DEFAULT_CLUSTER_ID)
        self.assertIsNotNone(state)
        if state["leader_node_id"] is not None:
            self.assertFalse(
                state["is_valid"],
                "Tests must not interrupt an active default-cluster leader.",
            )
            self.assertTrue(
                release_leadership(state["leader_node_id"], state["epoch"])
            )

    def test_01_initialization_is_idempotent_during_live_lease(self) -> None:
        initial = initialize_leadership(DEFAULT_CLUSTER_ID)
        self.assertIsNone(initial["leader_node_id"])
        self.assertFalse(initial["is_valid"])

        acquired = acquire_leadership(self.node_a, initial["epoch"])
        self.assertIsNotNone(acquired)
        try:
            if initial["epoch"] == 0:
                self.assertEqual(acquired["epoch"], 1)
            else:
                self.assertEqual(acquired["epoch"], initial["epoch"] + 1)

            repeated = initialize_leadership(DEFAULT_CLUSTER_ID)
            self.assertEqual(repeated["leader_node_id"], self.node_a)
            self.assertEqual(repeated["epoch"], acquired["epoch"])
            self.assertEqual(repeated["lease_until"], acquired["lease_until"])
            self.assertTrue(repeated["is_valid"])
        finally:
            release_leadership(self.node_a, acquired["epoch"])

    def test_02_acquisition_increments_the_observed_epoch_once(self) -> None:
        initial = get_leadership(DEFAULT_CLUSTER_ID)
        acquired = acquire_leadership(self.node_a, initial["epoch"])
        self.assertIsNotNone(acquired)
        try:
            self.assertEqual(acquired["epoch"], initial["epoch"] + 1)
            self.assertEqual(acquired["leader_node_id"], self.node_a)
            self.assertTrue(acquired["is_valid"])
        finally:
            release_leadership(self.node_a, acquired["epoch"])

    def test_03_second_owner_cannot_replace_a_live_leader(self) -> None:
        initial = get_leadership(DEFAULT_CLUSTER_ID)
        acquired = acquire_leadership(self.node_a, initial["epoch"])
        self.assertIsNotNone(acquired)
        try:
            rejected = acquire_leadership(self.node_b, initial["epoch"])
            self.assertIsNone(rejected)
            current = get_leadership(DEFAULT_CLUSTER_ID)
            self.assertEqual(current["leader_node_id"], self.node_a)
            self.assertEqual(current["epoch"], acquired["epoch"])
        finally:
            release_leadership(self.node_a, acquired["epoch"])

    def test_04_valid_renewal_extends_lease_without_incrementing_epoch(self) -> None:
        initial = get_leadership(DEFAULT_CLUSTER_ID)
        acquired = acquire_leadership(self.node_a, initial["epoch"])
        self.assertIsNotNone(acquired)
        try:
            time.sleep(0.01)
            renewed = renew_leadership(self.node_a, acquired["epoch"])
            self.assertIsNotNone(renewed)
            self.assertEqual(renewed["epoch"], acquired["epoch"])
            self.assertGreater(renewed["lease_until"], acquired["lease_until"])
            self.assertTrue(renewed["is_valid"])
        finally:
            release_leadership(self.node_a, acquired["epoch"])

    def test_05_expired_owner_cannot_renew_release_or_validate(self) -> None:
        initial = get_leadership(DEFAULT_CLUSTER_ID)
        owner_a = acquire_leadership(
            self.node_a,
            initial["epoch"],
            lease_duration_seconds=0.1,
        )
        self.assertIsNotNone(owner_a)
        time.sleep(0.15)

        owner_b = acquire_leadership(
            self.node_b,
            owner_a["epoch"],
            lease_duration_seconds=1,
        )
        self.assertIsNotNone(owner_b)
        try:
            self.assertEqual(owner_b["epoch"], owner_a["epoch"] + 1)
            self.assertIsNone(renew_leadership(self.node_a, owner_a["epoch"]))
            self.assertFalse(release_leadership(self.node_a, owner_a["epoch"]))
            self.assertFalse(validate_leadership(self.node_a, owner_a["epoch"]))
            self.assertTrue(validate_leadership(self.node_b, owner_b["epoch"]))
        finally:
            release_leadership(self.node_b, owner_b["epoch"])

    def test_06_epoch_is_monotonic_across_repeated_acquisitions(self) -> None:
        state = get_leadership(DEFAULT_CLUSTER_ID)
        epoch = state["epoch"]
        for node_id in (self.node_a, self.node_b, self.node_a):
            acquired = acquire_leadership(node_id, epoch)
            self.assertIsNotNone(acquired)
            self.assertEqual(acquired["epoch"], epoch + 1)
            epoch = acquired["epoch"]
            self.assertTrue(release_leadership(node_id, epoch))

        final_state = get_leadership(DEFAULT_CLUSTER_ID)
        self.assertEqual(final_state["epoch"], epoch)
        self.assertIsNone(final_state["leader_node_id"])
        self.assertFalse(final_state["is_valid"])

    def test_07_concurrent_acquisition_has_at_most_one_winner(self) -> None:
        initial = get_leadership(DEFAULT_CLUSTER_ID)
        barrier = threading.Barrier(3)

        def compete(node_id):
            barrier.wait(timeout=5)
            return acquire_leadership(node_id, initial["epoch"])

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(compete, self.node_a)
            second = executor.submit(compete, self.node_b)
            barrier.wait(timeout=5)
            results = (first.result(timeout=10), second.result(timeout=10))

        winners = [result for result in results if result is not None]
        self.assertEqual(len(winners), 1)
        winner = winners[0]
        try:
            current = get_leadership(DEFAULT_CLUSTER_ID)
            self.assertEqual(current["leader_node_id"], winner["leader_node_id"])
            self.assertEqual(current["epoch"], winner["epoch"])
        finally:
            release_leadership(winner["leader_node_id"], winner["epoch"])

    def test_08_state_persists_between_repository_connections(self) -> None:
        initial = get_leadership(DEFAULT_CLUSTER_ID)
        acquired = acquire_leadership(self.node_a, initial["epoch"])
        self.assertIsNotNone(acquired)
        try:
            persisted = get_leadership(DEFAULT_CLUSTER_ID)
            self.assertEqual(persisted["leader_node_id"], self.node_a)
            self.assertEqual(persisted["epoch"], acquired["epoch"])
            self.assertEqual(persisted["lease_until"], acquired["lease_until"])
        finally:
            release_leadership(self.node_a, acquired["epoch"])


if __name__ == "__main__":
    unittest.main()
