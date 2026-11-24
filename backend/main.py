import logging
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI
from fastapi.responses import JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from psycopg import Error as PsycopgError
from psycopg_pool import PoolTimeout

from api.cluster import router as cluster_router
from api.datasets import router as datasets_router
from api.jobs import router as jobs_router
from api.nodes import router as nodes_router
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
app.include_router(nodes_router)
app.include_router(datasets_router)
app.include_router(jobs_router)
app.include_router(cluster_router)


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


class FrontendFiles(StaticFiles):
    """Serves frontend/ asking the browser to revalidate every file.

    Without it a browser can keep an old JavaScript module after an update and
    mix it with new ones. Unchanged files still answer 304 via their ETag.
    """

    def file_response(self, *args: Any, **kwargs: Any) -> Response:
        response = super().file_response(*args, **kwargs)
        response.headers["Cache-Control"] = "no-cache"
        return response


# The interface (frontend/) is served from the same origin as the API, so the
# browser needs no CORS. Mounted last: every API route above takes precedence.
FRONTEND_DIR = Path(__file__).resolve().parent.parent / "frontend"
if FRONTEND_DIR.is_dir():
    app.mount("/", FrontendFiles(directory=FRONTEND_DIR, html=True), name="frontend")
