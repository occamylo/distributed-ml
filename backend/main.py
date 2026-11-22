import logging
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg import Error as PsycopgError
from psycopg_pool import PoolTimeout

from database.connection import (
    check_database_connection,
    close_pool,
    initialize_pool,
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    initialize_pool()
    try:
        yield
    finally:
        close_pool()


app = FastAPI(title="Distributed ML API", lifespan=lifespan)


@app.get("/health")
def health() -> JSONResponse:
    try:
        if check_database_connection():
            return JSONResponse(
                content={
                    "status": "healthy",
                    "application": "available",
                    "database": "available",
                }
            )
    except (PsycopgError, PoolTimeout) as exc:
        logger.warning("Database health check failed (%s).", type(exc).__name__)

    return JSONResponse(
        status_code=503,
        content={
            "status": "unhealthy",
            "application": "available",
            "database": "unavailable",
        },
    )
