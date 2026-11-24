import os
from typing import Any

from psycopg import Connection, Error as PsycopgError, OperationalError
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg_pool import ConnectionPool, PoolTimeout

_pool: ConnectionPool | None = None
_LOCAL_COCKROACH_PORTS = ("26257", "26258", "26259")
_LOCAL_CONNECT_TIMEOUT_SECONDS = 2


def _configured_database_url() -> str:
    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable",
    )
    if not database_url or not database_url.strip():
        raise ValueError("DATABASE_URL must be set to connect to CockroachDB.")
    return database_url


def _connection_infos(database_url: str) -> tuple[str, ...]:
    parameters = conninfo_to_dict(database_url)
    host = parameters.get("host")
    port = parameters.get("port")
    if host not in {"127.0.0.1", "localhost"} or port not in _LOCAL_COCKROACH_PORTS:
        return (database_url,)

    start = _LOCAL_COCKROACH_PORTS.index(port)
    ports = _LOCAL_COCKROACH_PORTS[start:] + _LOCAL_COCKROACH_PORTS[:start]
    return tuple(
        make_conninfo(**{**parameters, "port": candidate_port})
        for candidate_port in ports
    )


def get_cluster_connection_infos() -> tuple[str, ...]:
    return _connection_infos(_configured_database_url())


class _FailoverConnection(Connection):
    @classmethod
    def connect(cls, conninfo: str = "", **kwargs: Any) -> Connection:
        connection_infos = _connection_infos(conninfo)
        if len(connection_infos) == 1:
            return super().connect(conninfo, **kwargs)

        connect_kwargs = {
            **kwargs,
            "connect_timeout": _LOCAL_CONNECT_TIMEOUT_SECONDS,
        }
        last_error: OperationalError | None = None
        for connection_info in connection_infos:
            try:
                return super().connect(connection_info, **connect_kwargs)
            except OperationalError as error:
                last_error = error

        if last_error is None:
            raise OperationalError("No local CockroachDB endpoint was attempted.")
        raise last_error


def get_pool() -> ConnectionPool:
    if _pool is None:
        raise RuntimeError("The database connection pool is not initialized.")
    return _pool


def initialize_pool() -> None:
    global _pool

    if _pool is not None:
        raise RuntimeError("The database connection pool is already initialized.")

    database_url = _configured_database_url()

    pool = ConnectionPool(
        conninfo=database_url,
        connection_class=_FailoverConnection,
        min_size=1,
        max_size=10,
        timeout=10,
        kwargs={"connect_timeout": 10},
        check=ConnectionPool.check_connection,
        open=False,
    )
    try:
        pool.open(wait=True, timeout=10)
    except (PsycopgError, PoolTimeout):
        pool.close()
        raise

    _pool = pool


def close_pool() -> None:
    global _pool

    pool = _pool
    _pool = None
    if pool is not None:
        pool.close()


def check_database_connection() -> bool:
    pool = get_pool()
    with pool.connection() as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
        return result is not None and result[0] == 1
