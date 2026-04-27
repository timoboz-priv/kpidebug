import logging

from fastapi import FastAPI, Request

from kpidebug.common.logging import init_logging

init_logging()
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from kpidebug.config import config
from kpidebug.api.routes_users import router as users_router
from kpidebug.api.routes_projects import router as projects_router
from kpidebug.api.routes_metrics import router as metrics_router
from kpidebug.api.routes_data_sources import router as data_sources_router
from kpidebug.api.routes_data_tables import router as data_tables_router
from kpidebug.api.routes_dashboard import router as dashboard_router

app = FastAPI(title="KPI Debug", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[config.frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(users_router)
app.include_router(projects_router)
app.include_router(metrics_router)
app.include_router(data_sources_router)
app.include_router(data_tables_router)
app.include_router(dashboard_router)


logger = logging.getLogger(__name__)


@app.exception_handler(Exception)
async def unhandled_exception_handler(
    request: Request, exc: Exception,
) -> JSONResponse:
    logger.exception("Unhandled exception: %s", exc)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}
