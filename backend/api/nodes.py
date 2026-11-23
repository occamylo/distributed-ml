from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from database import repository

router = APIRouter()


class NodeRegistration(BaseModel):
    hostname: str
    ip_address: str | None = None
    cpu_cores: int | None = None
    ram_mb: int | None = None


@router.post("/nodes", status_code=status.HTTP_201_CREATED)
def register_node(node: NodeRegistration) -> dict[str, Any]:
    return repository.create_node(
        hostname=node.hostname,
        ip_address=node.ip_address,
        status="offline",
        cpu_cores=node.cpu_cores,
        ram_mb=node.ram_mb,
    )


@router.post("/nodes/{node_id}/heartbeat")
def receive_heartbeat(node_id: UUID) -> dict[str, Any]:
    updated_node = repository.update_node(
        node_id,
        {
            "last_heartbeat": datetime.now(timezone.utc),
            "status": "online",
        },
    )
    if updated_node is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Worker not found.",
        )

    return {"status": "received", "node_id": node_id}
