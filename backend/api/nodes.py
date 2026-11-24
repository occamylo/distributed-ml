from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from database import repository

router = APIRouter()


class NodeRegistration(BaseModel):
    node_id: UUID | None = None
    hostname: str
    ip_address: str | None = None
    cpu_cores: int | None = None
    ram_mb: int | None = None


@router.get("/nodes")
def list_workers() -> list[dict[str, Any]]:
    """Workers with their latest metrics, freshness and leadership role."""
    fresh_ids = {node["node_id"] for node in repository.list_nodes_with_fresh_metrics()}
    leadership = repository.get_leadership()
    leader_id = (
        leadership["leader_node_id"]
        if leadership is not None and leadership.get("is_valid")
        else None
    )
    now = datetime.now(timezone.utc)
    return [
        {
            **node,
            "is_fresh": node["node_id"] in fresh_ids,
            "is_leader": node["node_id"] == leader_id,
            "heartbeat_age_seconds": (
                round((now - node["last_heartbeat"]).total_seconds(), 1)
                if node.get("last_heartbeat") is not None
                else None
            ),
        }
        for node in repository.list_nodes()
    ]


@router.post("/nodes", status_code=status.HTTP_201_CREATED)
def register_node(node: NodeRegistration) -> dict[str, Any]:
    return repository.create_node(
        hostname=node.hostname,
        ip_address=node.ip_address,
        status="offline",
        cpu_cores=node.cpu_cores,
        ram_mb=node.ram_mb,
        node_id=node.node_id,
    )


@router.post("/nodes/{node_id}/heartbeat")
def receive_heartbeat(node_id: UUID) -> dict[str, Any]:
    updated_node = repository.update_node_heartbeat(node_id)
    if updated_node is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Worker not found.",
        )

    return {"status": "received", "node_id": node_id}
