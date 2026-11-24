"""Cluster state for the interface.

* ``/cluster/technical-status``: full technical view, built from the
  CockroachDB Admin API (node liveness, ranges per node, events) plus the
  application tables.
* ``/cluster``: lightweight summary used by the dashboard (SQL reachability
  of each node, replication from SHOW RANGES, leadership, Workers online).
"""

import json
import logging
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from json import JSONDecodeError
from threading import Lock
from time import perf_counter
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

from fastapi import APIRouter
from fastapi.concurrency import run_in_threadpool
from psycopg import Error as PsycopgError, OperationalError, connect
from psycopg.conninfo import conninfo_to_dict
from psycopg_pool import PoolTimeout

from database import repository
from database.connection import (
    check_database_connection,
    get_cluster_connection_infos,
    probe_database_hosts,
)
from database.repository import WORKER_STALE_AFTER_SECONDS

router = APIRouter()
logger = logging.getLogger(__name__)

_LOCAL_SQL_PORTS = ("26257", "26258", "26259")
_LOCAL_ADMIN_PORTS = ("8080", "8081", "8082")
_LOCAL_NODE_NAMES = ("cockroach1", "cockroach2", "cockroach3")
_RECENT_RECORD_LIMIT = 5
_SQL_CONNECT_TIMEOUT_SECONDS = 2
_ADMIN_API_TIMEOUT_SECONDS = 2
_CLUSTER_SNAPSHOT_TTL_SECONDS = 15
_CLUSTER_ERROR_RETRY_SECONDS = 4

_snapshot_lock = Lock()
_snapshot_cache_key: tuple[str, ...] | None = None
_snapshot_cache: dict[str, Any] | None = None
_snapshot_cache_expires_at = 0.0


def _configured_nodes() -> list[dict[str, Any]]:
    endpoints: list[dict[str, Any]] = []
    for conninfo in get_cluster_connection_infos():
        parameters = conninfo_to_dict(conninfo)
        host = parameters.get("host", "127.0.0.1")
        sql_port = parameters.get("port", "26257")
        admin_port: str | None = None
        node_name = host

        if host in {"127.0.0.1", "localhost"} and sql_port in _LOCAL_SQL_PORTS:
            index = _LOCAL_SQL_PORTS.index(sql_port)
            node_name = _LOCAL_NODE_NAMES[index]
            admin_port = _LOCAL_ADMIN_PORTS[index]
        elif host in _LOCAL_NODE_NAMES:
            index = _LOCAL_NODE_NAMES.index(host)
            node_name = host
            admin_port = _LOCAL_ADMIN_PORTS[index]

        if admin_port is not None:
            admin_host = "localhost" if host in {"127.0.0.1", "localhost"} else host
            formatted_host = f"[{admin_host}]" if ":" in admin_host else admin_host
            admin_url = f"http://{formatted_host}:{admin_port}"
        else:
            admin_url = None

        endpoints.append(
            {
                "name": node_name,
                "host": host,
                "sql_port": sql_port,
                "admin_port": int(admin_port) if admin_port else None,
                "admin_url": admin_url,
                "conninfo": conninfo,
            }
        )

    return sorted(endpoints, key=lambda endpoint: endpoint["sql_port"])


def _admin_candidates(endpoints: list[dict[str, Any]]) -> list[str]:
    candidates: list[str] = []
    for endpoint in endpoints:
        admin_url = endpoint["admin_url"]
        if admin_url and admin_url not in candidates:
            candidates.append(admin_url)
    return candidates


def _request_json(url: str) -> dict[str, Any]:
    request = Request(url, headers={"Accept": "application/json"})
    with urlopen(request, timeout=_ADMIN_API_TIMEOUT_SECONDS) as response:
        payload = json.loads(response.read().decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("CockroachDB API returned a non-object response.")
    return payload


def _error_message(error: Exception) -> str:
    if isinstance(error, HTTPError):
        return f"HTTP {error.code}"
    if isinstance(error, (URLError, TimeoutError)):
        return "No se pudo conectar al endpoint"
    if isinstance(error, JSONDecodeError):
        return "Respuesta JSON inválida"
    return "Respuesta inesperada del endpoint"


def _request_from_candidates(
    candidates: list[str],
    path: str,
) -> tuple[dict[str, Any] | None, str | None, Exception | None]:
    last_error: Exception | None = None
    for base_url in candidates:
        try:
            return _request_json(f"{base_url}{path}"), base_url, None
        except (HTTPError, URLError, TimeoutError, JSONDecodeError, ValueError) as error:
            last_error = error
    return None, None, last_error


def _range_summary(
    node_id: int,
    candidates: list[str],
) -> dict[str, Any]:
    payload, _, error = _request_from_candidates(
        candidates,
        f"/api/v2/nodes/{node_id}/ranges/?limit=1000",
    )
    if payload is None:
        logger.warning(
            "CockroachDB range status query failed for node %s (%s).",
            node_id,
            type(error).__name__ if error is not None else "UnknownError",
        )
        return {
            "node_id": node_id,
            "state": "error",
            "message": _error_message(error) if error is not None else "Sin respuesta",
        }

    ranges = payload.get("ranges")
    if not isinstance(ranges, list):
        return {
            "node_id": node_id,
            "state": "error",
            "message": "El endpoint no devolvió una lista de rangos",
        }

    problems = [
        item.get("problems") or {}
        for item in ranges
        if isinstance(item, dict)
    ]
    complete = payload.get("next") is None
    return {
        "node_id": node_id,
        "state": "available" if complete else "partial",
        "range_count": len(ranges),
        "unavailable_count": sum(problem.get("unavailable") is True for problem in problems),
        "underreplicated_count": sum(
            problem.get("underreplicated") is True for problem in problems
        ),
        "overreplicated_count": sum(
            problem.get("overreplicated") is True for problem in problems
        ),
        "complete": complete,
    }


def _load_cluster_snapshot(candidates: list[str]) -> dict[str, Any]:
    payload, admin_url, error = _request_from_candidates(
        candidates,
        "/api/v2/nodes/?limit=20",
    )
    if payload is None:
        message = _error_message(error) if error is not None else "Sin respuesta"
        logger.warning(
            "CockroachDB Cluster API node query failed (%s).",
            type(error).__name__ if error is not None else "UnknownError",
        )
        return {
            "nodes": {"state": "error", "message": message, "items": []},
            "ranges": {
                "state": "error",
                "message": "No se pudo consultar la lista de nodos",
                "items": [],
                "updated_at": datetime.now(timezone.utc),
            },
            "events": {
                "state": "error",
                "message": "No se pudo consultar la lista de nodos",
                "items": [],
                "updated_at": datetime.now(timezone.utc),
            },
        }

    raw_nodes = payload.get("nodes")
    if not isinstance(raw_nodes, list):
        return {
            "nodes": {
                "state": "error",
                "message": "El endpoint no devolvió una lista de nodos",
                "items": [],
            },
            "ranges": {
                "state": "error",
                "message": "Lista de nodos no disponible",
                "items": [],
                "updated_at": datetime.now(timezone.utc),
            },
            "events": {
                "state": "error",
                "message": "Lista de nodos no disponible",
                "items": [],
                "updated_at": datetime.now(timezone.utc),
            },
        }

    cluster_nodes = []
    for node in raw_nodes:
        if not isinstance(node, dict):
            continue
        address = node.get("address")
        address_field = address.get("address_field") if isinstance(address, dict) else None
        cluster_nodes.append(
            {
                "node_id": node.get("node_id"),
                "address": address_field,
                "build": node.get("build_tag"),
            }
        )

    snapshot_time = datetime.now(timezone.utc)
    node_list_state = "available" if payload.get("next") is None else "partial"
    with ThreadPoolExecutor(max_workers=3) as executor:
        range_items = list(
            executor.map(
                lambda node: _range_summary(node["node_id"], [admin_url]),
                (
                    node
                    for node in cluster_nodes
                    if isinstance(node.get("node_id"), int)
                    and not isinstance(node.get("node_id"), bool)
                ),
            )
        )

    range_state = (
        "available"
        if range_items and all(item["state"] == "available" for item in range_items)
        else "partial"
        if any(item["state"] in {"available", "partial"} for item in range_items)
        else "error"
    )

    events_payload, _, events_error = _request_from_candidates(
        [admin_url],
        "/api/v2/events/?limit=5",
    )
    if events_payload is None or not isinstance(events_payload.get("events"), list):
        events_state = "error"
        event_items = []
        events_message = (
            _error_message(events_error)
            if events_error is not None
            else "El endpoint no devolvió una lista de eventos"
        )
        logger.warning(
            "CockroachDB event query failed (%s).",
            type(events_error).__name__ if events_error is not None else "InvalidResponse",
        )
    else:
        events_state = "available"
        events_message = None
        event_items = [
            {
                "timestamp": event.get("timestamp"),
                "event_type": event.get("event_type"),
                "reporting_id": event.get("reporting_id"),
                "target_id": event.get("target_id"),
            }
            for event in events_payload["events"]
            if isinstance(event, dict)
        ]

    return {
        "nodes": {
            "state": node_list_state,
            "items": cluster_nodes,
            "updated_at": snapshot_time,
        },
        "ranges": {
            "state": range_state,
            "items": range_items,
            "message": (
                "El endpoint no devolvió rangos completos"
                if range_state == "partial"
                else None
            ),
            "updated_at": snapshot_time,
        },
        "events": {
            "state": events_state,
            "items": event_items,
            "message": events_message,
            "updated_at": snapshot_time,
        },
    }


def _get_cluster_snapshot(candidates: list[str]) -> dict[str, Any]:
    global _snapshot_cache_key
    global _snapshot_cache
    global _snapshot_cache_expires_at

    cache_key = tuple(candidates)
    now = time.monotonic()
    with _snapshot_lock:
        if (
            _snapshot_cache is not None
            and _snapshot_cache_key == cache_key
            and now < _snapshot_cache_expires_at
        ):
            return _snapshot_cache

        snapshot = _load_cluster_snapshot(candidates)
        _snapshot_cache_key = cache_key
        _snapshot_cache = snapshot
        ttl = (
            _CLUSTER_SNAPSHOT_TTL_SECONDS
            if snapshot["nodes"]["state"] == "available"
            else _CLUSTER_ERROR_RETRY_SECONDS
        )
        _snapshot_cache_expires_at = time.monotonic() + ttl
        return snapshot


def _probe_sql_endpoint(endpoint: dict[str, Any]) -> dict[str, Any]:
    started = perf_counter()
    try:
        with connect(
            endpoint["conninfo"],
            connect_timeout=_SQL_CONNECT_TIMEOUT_SECONDS,
            autocommit=True,
        ) as connection:
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                row = cursor.fetchone()
        if row is None or row[0] != 1:
            return {
                "state": "error",
                "message": "La consulta SELECT 1 no devolvió el resultado esperado",
            }
        return {
            "state": "available",
            "query": "SELECT 1",
            "result": row[0],
            "latency_ms": round((perf_counter() - started) * 1000, 1),
        }
    except OperationalError:
        return {
            "state": "unavailable",
            "message": "No fue posible abrir una conexión SQL",
            "latency_ms": round((perf_counter() - started) * 1000, 1),
        }
    except (PsycopgError, OSError, TimeoutError, ValueError) as error:
        logger.warning(
            "CockroachDB SQL probe failed for %s (%s).",
            endpoint["name"],
            type(error).__name__,
        )
        return {
            "state": "error",
            "message": "Falló la consulta SQL",
            "latency_ms": round((perf_counter() - started) * 1000, 1),
        }


def _probe_admin_health(endpoint: dict[str, Any]) -> dict[str, Any]:
    admin_url = endpoint["admin_url"]
    if admin_url is None:
        return {
            "state": "unavailable",
            "message": "No hay una dirección Admin API configurada",
        }
    try:
        _request_json(f"{admin_url}/api/v2/health/")
        return {"state": "ready"}
    except HTTPError as error:
        state = "not_ready" if error.code == 503 else "error"
        return {"state": state, "message": f"HTTP {error.code}"}
    except (URLError, TimeoutError) as error:
        logger.warning(
            "CockroachDB Admin API health query failed for %s (%s).",
            endpoint["name"],
            type(error).__name__,
        )
        return {"state": "unavailable", "message": "Admin API no accesible"}
    except (JSONDecodeError, ValueError) as error:
        logger.warning(
            "CockroachDB Admin API health response was invalid for %s (%s).",
            endpoint["name"],
            type(error).__name__,
        )
        return {"state": "error", "message": "Respuesta Admin API inválida"}


def _host_from_address(address: Any) -> str | None:
    if not isinstance(address, str) or not address:
        return None
    host = address.rsplit(":", 1)[0]
    return host.strip("[]")


def _worker_status(node: dict[str, Any], now: datetime) -> str:
    reported_status = node.get("status")
    last_heartbeat = node.get("last_heartbeat")
    if not isinstance(last_heartbeat, datetime):
        return "offline"
    if last_heartbeat.tzinfo is None:
        last_heartbeat = last_heartbeat.replace(tzinfo=timezone.utc)
    if (now - last_heartbeat).total_seconds() > WORKER_STALE_AFTER_SECONDS:
        return "offline"
    if reported_status in {"online", "busy"}:
        return reported_status
    return "offline" if reported_status == "offline" else "unknown"


def _read_section(loader: Any, label: str) -> dict[str, Any]:
    try:
        return {"state": "available", "items": loader()}
    except (PsycopgError, PoolTimeout) as error:
        logger.warning(
            "Technical status query for %s failed (%s).",
            label,
            type(error).__name__,
        )
        return {
            "state": "error",
            "items": [],
            "message": "Falló la consulta a CockroachDB",
        }


def _leadership_status(
    workers: list[dict[str, Any]],
) -> dict[str, Any]:
    try:
        leadership = repository.get_leadership()
    except (PsycopgError, PoolTimeout) as error:
        logger.warning(
            "Technical leadership query failed (%s).",
            type(error).__name__,
        )
        return {
            "state": "error",
            "message": "Falló la consulta del lease de liderazgo",
        }

    if leadership is None:
        return {
            "state": "unavailable",
            "message": "No existe un registro de liderazgo para el clúster",
        }

    leader_node_id = leadership.get("leader_node_id")
    if leader_node_id is None or not leadership.get("is_valid"):
        return {
            "state": "no_leader",
            "epoch": leadership.get("epoch"),
            "lease_until": leadership.get("lease_until"),
        }

    worker = next(
        (item for item in workers if item.get("node_id") == leader_node_id),
        None,
    )
    return {
        "state": "leader",
        "epoch": leadership.get("epoch"),
        "lease_until": leadership.get("lease_until"),
        "worker": {
            "node_id": leader_node_id,
            "hostname": worker.get("hostname") if worker else None,
            "status": worker.get("status") if worker else None,
        },
    }


@router.get("/cluster/technical-status")
def read_technical_status() -> dict[str, Any]:
    endpoints = _configured_nodes()
    candidates = _admin_candidates(endpoints)
    with ThreadPoolExecutor(max_workers=max(1, len(endpoints) * 2)) as executor:
        sql_probes = list(executor.map(_probe_sql_endpoint, endpoints))
        admin_health = list(executor.map(_probe_admin_health, endpoints))

    snapshot = _get_cluster_snapshot(candidates)
    cluster_nodes = snapshot["nodes"]["items"]
    cluster_nodes_by_host = {
        host: node
        for node in cluster_nodes
        if (host := _host_from_address(node.get("address"))) is not None
    }

    database_nodes = []
    for endpoint, sql_probe, health in zip(endpoints, sql_probes, admin_health):
        member = cluster_nodes_by_host.get(endpoint["name"])
        if snapshot["nodes"]["state"] == "error":
            membership = {
                "state": "error",
                "message": snapshot["nodes"]["message"],
            }
        elif member is None:
            membership = {
                "state": (
                    "unavailable"
                    if snapshot["nodes"]["state"] == "partial"
                    else "not_listed"
                ),
                "message": (
                    "La respuesta paginada no incluye este endpoint"
                    if snapshot["nodes"]["state"] == "partial"
                    else None
                ),
            }
        else:
            membership = {
                "state": "member",
                "node_id": member["node_id"],
                "address": member["address"],
                "build": member["build"],
            }

        database_nodes.append(
            {
                "name": endpoint["name"],
                "sql_port": endpoint["sql_port"],
                "admin_port": endpoint["admin_port"],
                "sql": sql_probe,
                "membership": membership,
                "health": health,
            }
        )

    worker_section = _read_section(repository.list_nodes, "Workers")
    workers: list[dict[str, Any]] = []
    if worker_section["state"] == "available":
        now = datetime.now(timezone.utc)
        for node in worker_section["items"]:
            workers.append(
                {
                    "node_id": node.get("node_id"),
                    "hostname": node.get("hostname"),
                    "status": _worker_status(node, now),
                    "reported_status": node.get("status"),
                    "last_heartbeat": node.get("last_heartbeat"),
                    "active_tasks": node.get("active_tasks"),
                    "cpu_cores": node.get("cpu_cores"),
                    "ram_mb": node.get("ram_mb"),
                }
            )

    leadership = _leadership_status(workers)
    jobs = _read_section(
        lambda: repository.list_recent_jobs(_RECENT_RECORD_LIMIT),
        "Jobs",
    )
    tasks = _read_section(
        lambda: repository.list_recent_tasks(_RECENT_RECORD_LIMIT),
        "Tasks",
    )
    datasets = _read_section(
        lambda: repository.list_recent_datasets(_RECENT_RECORD_LIMIT),
        "Datasets",
    )

    try:
        pool_available = check_database_connection()
        pool_state = "available" if pool_available else "error"
    except (PsycopgError, PoolTimeout) as error:
        logger.warning(
            "Technical database pool check failed (%s).",
            type(error).__name__,
        )
        pool_state = "error"

    sql_available_count = sum(
        node["sql"]["state"] == "available" for node in database_nodes
    )
    all_members = (
        snapshot["nodes"]["state"] == "available"
        and len(database_nodes) == len(_LOCAL_NODE_NAMES)
        and all(node["membership"]["state"] == "member" for node in database_nodes)
    )
    all_admin_health_ready = all(
        node["health"]["state"] == "ready" for node in database_nodes
    )
    if (
        pool_state == "available"
        and sql_available_count == len(_LOCAL_NODE_NAMES)
        and all_members
        and all_admin_health_ready
    ):
        overall_state = "healthy"
    elif sql_available_count or pool_state == "available":
        overall_state = "degraded"
    else:
        overall_state = "unavailable"

    return {
        "updated_at": datetime.now(timezone.utc),
        "refresh_interval_seconds": 4,
        "overall": {
            "state": overall_state,
            "application": "available",
            "database_pool": pool_state,
        },
        "database_nodes": {
            "state": snapshot["nodes"]["state"],
            "updated_at": snapshot["nodes"].get("updated_at"),
            "items": database_nodes,
        },
        "replication": {
            "state": snapshot["ranges"]["state"],
            "source": "CockroachDB Cluster API v2 /nodes/{node_id}/ranges/",
            "updated_at": snapshot["ranges"]["updated_at"],
            "message": snapshot["ranges"].get("message"),
            "items": snapshot["ranges"]["items"],
            "interpretation": (
                "Conteos y alertas reportados por nodo; no demuestran por sí solos "
                "el factor de replicación ni la consistencia física completa."
            ),
        },
        "leadership": leadership,
        "workers": {
            **worker_section,
            "items": workers,
            "freshness_seconds": WORKER_STALE_AFTER_SECONDS,
        },
        "jobs": jobs,
        "tasks": {
            **tasks,
            "timestamp_note": (
                "Tasks no tiene created_at: las pendientes se ordenan por la fecha "
                "de creación del Job asociado."
            ),
        },
        "datasets": datasets,
        "events": {
            **snapshot["events"],
            "source": "CockroachDB Cluster API v2 /events/ (inestable)",
        },
    }


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
