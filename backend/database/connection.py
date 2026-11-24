import os

from psycopg import Error as PsycopgError
from psycopg_pool import ConnectionPool, PoolTimeout

# libpq tries the hosts in order, so losing the first CockroachDB node does
# not take the application down while the other two still hold quorum.
DEFAULT_DATABASE_URL = (
    "postgresql://root@127.0.0.1:26257,127.0.0.1:26258,127.0.0.1:26259/"
    "distributed_ml?sslmode=disable"
)
# Applied per host attempt; keeps the worst-case failover across three hosts
# below the 10-second pool timeout.
CONNECT_TIMEOUT_SECONDS = 3
# Without quorum CockroachDB keeps statements waiting indefinitely; the
# timeout turns that wait into an error the callers already handle.
STATEMENT_TIMEOUT_MS = 5000
POOL_TIMEOUT_SECONDS = 10
HEALTH_CHECK_TIMEOUT_SECONDS = 5

_pool: ConnectionPool | None = None


def get_pool() -> ConnectionPool:
    if _pool is None:
        raise RuntimeError("The database connection pool is not initialized.")
    return _pool


def initialize_pool() -> None:
    global _pool

    if _pool is not None:
        raise RuntimeError("The database connection pool is already initialized.")

    database_url = os.environ.get("DATABASE_URL", DEFAULT_DATABASE_URL)
    if not database_url or not database_url.strip():
        raise ValueError("DATABASE_URL must be set to connect to CockroachDB.")

    pool = ConnectionPool(
        conninfo=database_url,
        min_size=1,
        max_size=10,
        timeout=POOL_TIMEOUT_SECONDS,
        kwargs={
            "connect_timeout": CONNECT_TIMEOUT_SECONDS,
            "options": f"-c statement_timeout={STATEMENT_TIMEOUT_MS}",
        },
        # Discards connections to a node that went down before handing them out.
        check=ConnectionPool.check_connection,
        open=False,
    )
    try:
        pool.open(wait=True, timeout=POOL_TIMEOUT_SECONDS)
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
    # Reads a replicated table: a bare SELECT 1 succeeds even without quorum.
    pool = get_pool()
    with pool.connection(timeout=HEALTH_CHECK_TIMEOUT_SECONDS) as connection:
        with connection.cursor() as cursor:
            cursor.execute("SELECT EXISTS (SELECT 1 FROM nodes LIMIT 1)")
            result = cursor.fetchone()
        return result is not None
