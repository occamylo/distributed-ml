"""State of both clusters for the interface: database nodes and Workers."""

from collections import Counter
from typing import Any

from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from psycopg import Error as PsycopgError
from psycopg_pool import PoolTimeout

from database import repository
from database.connection import probe_database_hosts

router = APIRouter()


@router.get("/cluster")
async def read_cluster() -> dict[str, Any]:
    nodes = await run_in_threadpool(probe_database_hosts)
    reachable = sum(node["reachable"] for node in nodes)

    # Replication and leadership need a quorum; without it they are reported
    # as unavailable instead of failing the whole response.
    replication: dict[str, Any] | None = None
    leadership: dict[str, Any] | None = None
    workers: dict[str, int] | None = None
    try:
        ranges = await run_in_threadpool(repository.list_database_ranges)
        replica_counts = Counter(len(item["replicas"] or []) for item in ranges)
        replication = {
            "ranges": len(ranges),
            "replica_counts": {str(k): v for k, v in sorted(replica_counts.items())},
            "replica_nodes": sorted({n for item in ranges for n in (item["replicas"] or [])}),
            "lease_holders": dict(
                sorted(Counter(str(item["lease_holder"]) for item in ranges).items())
            ),
        }
        lease = await run_in_threadpool(repository.get_leadership)
        if lease is not None:
            leadership = {
                "leader_node_id": lease["leader_node_id"] if lease["is_valid"] else None,
                "epoch": lease["epoch"],
                "lease_until": lease["lease_until"],
                "is_valid": lease["is_valid"],
            }
        fresh = await run_in_threadpool(repository.list_nodes_with_fresh_metrics)
        registered = await run_in_threadpool(repository.list_nodes)
        workers = {
            "online": sum(1 for node in fresh if node.get("status") in ("online", "busy")),
            "registered": len(registered),
        }
    except (PsycopgError, PoolTimeout):
        pass

    return {
        "database": {
            "nodes": nodes,
            "reachable": reachable,
            "total": len(nodes),
            "quorum": reachable >= len(nodes) // 2 + 1,
            "replication": replication,
        },
        "leadership": leadership,
        "workers": workers,
    }
