import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

from api.cluster import (
    _leadership_status,
    _probe_sql_endpoint,
    _range_summary,
    _worker_status,
)
from database.repository import WORKER_STALE_AFTER_SECONDS


class TechnicalStatusTests(unittest.TestCase):
    def test_worker_with_stale_heartbeat_is_offline(self) -> None:
        now = datetime.now(timezone.utc)
        node = {
            "status": "online",
            "last_heartbeat": now - timedelta(
                seconds=WORKER_STALE_AFTER_SECONDS + 1
            ),
        }

        self.assertEqual(_worker_status(node, now), "offline")

    def test_fresh_busy_worker_keeps_reported_status(self) -> None:
        now = datetime.now(timezone.utc)
        node = {"status": "busy", "last_heartbeat": now}

        self.assertEqual(_worker_status(node, now), "busy")

    def test_refused_sql_endpoint_is_reported_unavailable(self) -> None:
        status = _probe_sql_endpoint(
            {
                "name": "unreachable-test-endpoint",
                "conninfo": (
                    "host=127.0.0.1 port=1 user=root "
                    "dbname=distributed_ml sslmode=disable"
                ),
            }
        )

        self.assertEqual(status["state"], "unavailable")

    def test_missing_leadership_record_is_not_reported_as_no_leader(self) -> None:
        with patch("api.cluster.repository.get_leadership", return_value=None):
            status = _leadership_status([])

        self.assertEqual(status["state"], "unavailable")

    def test_expired_leadership_is_reported_as_no_leader(self) -> None:
        with patch(
            "api.cluster.repository.get_leadership",
            return_value={
                "leader_node_id": uuid4(),
                "epoch": 4,
                "lease_until": datetime.now(timezone.utc) - timedelta(seconds=1),
                "is_valid": False,
            },
        ):
            status = _leadership_status([])

        self.assertEqual(status["state"], "no_leader")

    def test_current_leadership_uses_the_recorded_worker(self) -> None:
        node_id = uuid4()
        with patch(
            "api.cluster.repository.get_leadership",
            return_value={
                "leader_node_id": node_id,
                "epoch": 5,
                "lease_until": datetime.now(timezone.utc) + timedelta(seconds=5),
                "is_valid": True,
            },
        ):
            status = _leadership_status(
                [{"node_id": node_id, "hostname": "worker-a", "status": "online"}]
            )

        self.assertEqual(status["state"], "leader")
        self.assertEqual(status["worker"]["node_id"], node_id)
        self.assertEqual(status["worker"]["hostname"], "worker-a")

    def test_range_report_counts_only_reported_problems(self) -> None:
        payload = {
            "ranges": [
                {"problems": {"underreplicated": True, "unavailable": False}},
                {"problems": {"underreplicated": False, "unavailable": True}},
            ],
            "next": None,
        }
        with patch(
            "api.cluster._request_from_candidates",
            return_value=(payload, "http://localhost:8081", None),
        ):
            status = _range_summary(2, ["http://localhost:8081"])

        self.assertEqual(status["state"], "available")
        self.assertEqual(status["range_count"], 2)
        self.assertEqual(status["underreplicated_count"], 1)
        self.assertEqual(status["unavailable_count"], 1)


if __name__ == "__main__":
    unittest.main()
