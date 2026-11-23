import math
import time
from collections.abc import Callable, Mapping, Sequence
from datetime import datetime, timedelta
from typing import Any, TypeVar
from uuid import UUID

from psycopg import sql
from psycopg.errors import SerializationFailure
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb

from database.connection import get_pool

Record = dict[str, Any]
DEFAULT_CLUSTER_ID = "default"
DEFAULT_LEASE_DURATION_SECONDS = 10.0
MAX_SERIALIZATION_ATTEMPTS = 5
_T = TypeVar("_T")

_NODE_UPDATE_FIELDS = (
    "hostname",
    "ip_address",
    "status",
    "cpu_cores",
    "ram_mb",
    "last_heartbeat",
)
_JOB_UPDATE_FIELDS = (
    "dataset_id",
    "algorithm",
    "target_column",
    "status",
    "parameters",
    "requested_workers",
    "actual_workers",
    "train_ratio",
    "seed",
    "error_message",
    "started_at",
    "completed_at",
)
_TASK_UPDATE_FIELDS = (
    "job_id",
    "node_id",
    "partition_index",
    "rows_assigned",
    "status",
    "attempts",
    "started_at",
    "completed_at",
    "error_message",
)
_LEADERSHIP_SELECT = (
    "SELECT cluster_id, leader_node_id, epoch, lease_until, updated_at, "
    "lease_until > now() AS is_valid "
    "FROM cluster_leadership WHERE cluster_id = %s"
)


def _fetch_one(query: str | sql.Composable, params: Sequence[Any]) -> Record | None:
    with get_pool().connection() as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            return cursor.fetchone()


def _fetch_all(query: str | sql.Composable, params: Sequence[Any] = ()) -> list[Record]:
    with get_pool().connection() as connection:
        with connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, params)
            return list(cursor.fetchall())


def _run_leadership_transaction(operation: Callable[[Any], _T]) -> _T:
    for attempt in range(MAX_SERIALIZATION_ATTEMPTS):
        try:
            with get_pool().connection() as connection:
                with connection.transaction():
                    with connection.cursor(row_factory=dict_row) as cursor:
                        return operation(cursor)
        except SerializationFailure:
            if attempt + 1 == MAX_SERIALIZATION_ATTEMPTS:
                raise
            time.sleep(min(0.01 * (2**attempt), 0.1))
    raise RuntimeError("Leadership transaction exhausted without a result.")


def _lease_duration(seconds: float) -> timedelta:
    if not math.isfinite(seconds) or seconds <= 0:
        raise ValueError("Lease duration must be a finite value greater than zero.")
    duration = timedelta(seconds=seconds)
    if duration <= timedelta(0):
        raise ValueError("Lease duration is below the supported timestamp precision.")
    return duration


def initialize_leadership(cluster_id: str = DEFAULT_CLUSTER_ID) -> Record:
    def initialize(cursor: Any) -> Record:
        cursor.execute(
            "INSERT INTO cluster_leadership "
            "(cluster_id, leader_node_id, epoch, lease_until, updated_at) "
            "VALUES (%s, NULL, 0, now(), now()) "
            "ON CONFLICT (cluster_id) DO NOTHING",
            (cluster_id,),
        )
        cursor.execute(_LEADERSHIP_SELECT, (cluster_id,))
        row = cursor.fetchone()
        if row is None:
            raise RuntimeError(f"Leadership row {cluster_id!r} was not initialized.")
        return row

    return _run_leadership_transaction(initialize)


def get_leadership(cluster_id: str = DEFAULT_CLUSTER_ID) -> Record | None:
    return _fetch_one(_LEADERSHIP_SELECT, (cluster_id,))


def acquire_leadership(
    node_id: UUID,
    observed_epoch: int,
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    cluster_id: str = DEFAULT_CLUSTER_ID,
) -> Record | None:
    duration = _lease_duration(lease_duration_seconds)

    def acquire(cursor: Any) -> Record | None:
        cursor.execute(
            "UPDATE cluster_leadership "
            "SET leader_node_id = %s, epoch = epoch + 1, "
            "lease_until = now() + %s, updated_at = now() "
            "WHERE cluster_id = %s AND epoch = %s "
            "AND (leader_node_id IS NULL OR lease_until <= now()) "
            "RETURNING cluster_id, leader_node_id, epoch, lease_until, updated_at, "
            "lease_until > now() AS is_valid",
            (node_id, duration, cluster_id, observed_epoch),
        )
        return cursor.fetchone()

    return _run_leadership_transaction(acquire)


def renew_leadership(
    node_id: UUID,
    epoch: int,
    lease_duration_seconds: float = DEFAULT_LEASE_DURATION_SECONDS,
    cluster_id: str = DEFAULT_CLUSTER_ID,
) -> Record | None:
    duration = _lease_duration(lease_duration_seconds)

    def renew(cursor: Any) -> Record | None:
        cursor.execute(
            "UPDATE cluster_leadership "
            "SET lease_until = now() + %s, updated_at = now() "
            "WHERE cluster_id = %s AND leader_node_id = %s AND epoch = %s "
            "AND lease_until > now() "
            "RETURNING cluster_id, leader_node_id, epoch, lease_until, updated_at, "
            "lease_until > now() AS is_valid",
            (duration, cluster_id, node_id, epoch),
        )
        return cursor.fetchone()

    return _run_leadership_transaction(renew)


def release_leadership(
    node_id: UUID,
    epoch: int,
    cluster_id: str = DEFAULT_CLUSTER_ID,
) -> bool:
    def release(cursor: Any) -> bool:
        cursor.execute(
            "UPDATE cluster_leadership "
            "SET leader_node_id = NULL, lease_until = now(), updated_at = now() "
            "WHERE cluster_id = %s AND leader_node_id = %s AND epoch = %s "
            "RETURNING cluster_id",
            (cluster_id, node_id, epoch),
        )
        return cursor.fetchone() is not None

    return _run_leadership_transaction(release)


def validate_leadership(
    node_id: UUID,
    epoch: int,
    cluster_id: str = DEFAULT_CLUSTER_ID,
) -> bool:
    row = _fetch_one(
        "SELECT EXISTS ("
        "SELECT 1 FROM cluster_leadership "
        "WHERE cluster_id = %s AND leader_node_id = %s AND epoch = %s "
        "AND lease_until > now()"
        ") AS is_valid",
        (cluster_id, node_id, epoch),
    )
    if row is None:
        raise RuntimeError("Leadership validation did not return a result.")
    return row["is_valid"]


def _insert(table: str, values: Mapping[str, Any]) -> Record:
    columns = tuple(values)
    query = sql.SQL("INSERT INTO {} ({}) VALUES ({}) RETURNING *").format(
        sql.Identifier(table),
        sql.SQL(", ").join(sql.Identifier(column) for column in columns),
        sql.SQL(", ").join(sql.Placeholder() for _ in columns),
    )
    row = _fetch_one(query, tuple(values[column] for column in columns))
    if row is None:
        raise RuntimeError(f"INSERT into {table} did not return the created row.")
    return row


def _get_one(table: str, id_column: str, entity_id: UUID) -> Record | None:
    query = sql.SQL("SELECT * FROM {} WHERE {} = %s").format(
        sql.Identifier(table),
        sql.Identifier(id_column),
    )
    return _fetch_one(query, (entity_id,))


def _update(
    table: str,
    id_column: str,
    entity_id: UUID,
    fields: Mapping[str, Any],
    allowed_fields: Sequence[str],
    json_fields: Sequence[str] = (),
    touch_updated_at: bool = False,
) -> Record | None:
    unknown_fields = set(fields).difference(allowed_fields)
    if unknown_fields:
        names = ", ".join(sorted(unknown_fields))
        raise ValueError(f"Unsupported update field(s) for {table}: {names}")

    if not fields:
        return _get_one(table, id_column, entity_id)

    assignments = [
        sql.SQL("{} = %s").format(sql.Identifier(column))
        for column in fields
    ]
    if touch_updated_at:
        assignments.append(sql.SQL("{} = now()").format(sql.Identifier("updated_at")))

    query = sql.SQL("UPDATE {} SET {} WHERE {} = %s RETURNING *").format(
        sql.Identifier(table),
        sql.SQL(", ").join(assignments),
        sql.Identifier(id_column),
    )
    values = tuple(
        Jsonb(value) if column in json_fields else value
        for column, value in fields.items()
    )
    return _fetch_one(query, (*values, entity_id))


def create_node(
    hostname: str,
    ip_address: str | None = None,
    status: str = "offline",
    cpu_cores: int | None = None,
    ram_mb: int | None = None,
    last_heartbeat: datetime | None = None,
) -> Record:
    return _insert(
        "nodes",
        {
            "hostname": hostname,
            "ip_address": ip_address,
            "status": status,
            "cpu_cores": cpu_cores,
            "ram_mb": ram_mb,
            "last_heartbeat": last_heartbeat,
        },
    )


def get_node(node_id: UUID) -> Record | None:
    return _get_one("nodes", "node_id", node_id)


def list_nodes() -> list[Record]:
    return _fetch_all(
        "SELECT * FROM nodes ORDER BY created_at ASC, node_id ASC"
    )


def update_node(node_id: UUID, fields: Mapping[str, Any]) -> Record | None:
    return _update(
        "nodes",
        "node_id",
        node_id,
        fields,
        _NODE_UPDATE_FIELDS,
        touch_updated_at=True,
    )


def create_dataset(
    name: str,
    file_path: str,
    file_size_bytes: int | None = None,
    row_count: int | None = None,
    column_count: int | None = None,
    file_format: str = "csv",
) -> Record:
    return _insert(
        "datasets",
        {
            "name": name,
            "file_path": file_path,
            "file_size_bytes": file_size_bytes,
            "row_count": row_count,
            "column_count": column_count,
            "file_format": file_format,
        },
    )


def get_dataset(dataset_id: UUID) -> Record | None:
    return _get_one("datasets", "dataset_id", dataset_id)


def list_datasets() -> list[Record]:
    return _fetch_all(
        "SELECT * FROM datasets ORDER BY created_at ASC, dataset_id ASC"
    )


def delete_dataset(dataset_id: UUID) -> bool:
    query = sql.SQL("DELETE FROM {} WHERE {} = %s RETURNING {}").format(
        sql.Identifier("datasets"),
        sql.Identifier("dataset_id"),
        sql.Identifier("dataset_id"),
    )
    return _fetch_one(query, (dataset_id,)) is not None


def create_job(
    dataset_id: UUID,
    algorithm: str,
    target_column: str,
    requested_workers: int,
    status: str = "pending",
    parameters: Any = None,
    actual_workers: int | None = None,
    train_ratio: float | None = None,
    seed: int | None = None,
    error_message: str | None = None,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
) -> Record:
    return _insert(
        "jobs",
        {
            "dataset_id": dataset_id,
            "algorithm": algorithm,
            "target_column": target_column,
            "status": status,
            "parameters": Jsonb({} if parameters is None else parameters),
            "requested_workers": requested_workers,
            "actual_workers": actual_workers,
            "train_ratio": train_ratio,
            "seed": seed,
            "error_message": error_message,
            "started_at": started_at,
            "completed_at": completed_at,
        },
    )


def get_job(job_id: UUID) -> Record | None:
    return _get_one("jobs", "job_id", job_id)


def list_jobs() -> list[Record]:
    return _fetch_all("SELECT * FROM jobs ORDER BY created_at ASC, job_id ASC")


def update_job(job_id: UUID, fields: Mapping[str, Any]) -> Record | None:
    return _update(
        "jobs",
        "job_id",
        job_id,
        fields,
        _JOB_UPDATE_FIELDS,
        json_fields=("parameters",),
    )


def create_task(
    job_id: UUID,
    partition_index: int,
    node_id: UUID | None = None,
    rows_assigned: int | None = None,
    status: str = "pending",
    attempts: int = 0,
    started_at: datetime | None = None,
    completed_at: datetime | None = None,
    error_message: str | None = None,
) -> Record:
    return _insert(
        "tasks",
        {
            "job_id": job_id,
            "node_id": node_id,
            "partition_index": partition_index,
            "rows_assigned": rows_assigned,
            "status": status,
            "attempts": attempts,
            "started_at": started_at,
            "completed_at": completed_at,
            "error_message": error_message,
        },
    )


def get_task(task_id: UUID) -> Record | None:
    return _get_one("tasks", "task_id", task_id)


def list_tasks_by_job(job_id: UUID) -> list[Record]:
    return _fetch_all(
        "SELECT * FROM tasks "
        "WHERE job_id = %s "
        "ORDER BY partition_index ASC, task_id ASC",
        (job_id,),
    )


def update_task(task_id: UUID, fields: Mapping[str, Any]) -> Record | None:
    return _update(
        "tasks",
        "task_id",
        task_id,
        fields,
        _TASK_UPDATE_FIELDS,
    )


def create_result(
    job_id: UUID,
    duration_seconds: float,
    accuracy: float | None = None,
    loss: float | None = None,
    metrics: Any = None,
    model_path: str | None = None,
) -> Record:
    return _insert(
        "results",
        {
            "job_id": job_id,
            "accuracy": accuracy,
            "loss": loss,
            "duration_seconds": duration_seconds,
            "metrics": Jsonb({} if metrics is None else metrics),
            "model_path": model_path,
        },
    )


def get_result(result_id: UUID) -> Record | None:
    return _get_one("results", "result_id", result_id)


def get_result_by_job(job_id: UUID) -> Record | None:
    return _fetch_one(
        "SELECT * FROM results WHERE job_id = %s",
        (job_id,),
    )
