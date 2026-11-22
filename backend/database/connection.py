import os

from psycopg import Error as PsycopgError
from psycopg_pool import ConnectionPool, PoolTimeout

_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    if _pool is None:
        raise RuntimeError("The database connection pool is not initialized.")
    return _pool


def initialize_pool() -> None:
    global _pool

    if _pool is not None:
        raise RuntimeError("The database connection pool is already initialized.")

    database_url = os.environ.get(
        "DATABASE_URL",
        "postgresql://root@127.0.0.1:26257/distributed_ml?sslmode=disable",
    )
    if not database_url or not database_url.strip():
        raise ValueError("DATABASE_URL must be set to connect to CockroachDB.")

    pool = ConnectionPool(
        conninfo=database_url,
        min_size=1,
        max_size=10,
        timeout=10,
        kwargs={"connect_timeout": 10},
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
