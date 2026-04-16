from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from kpidebug.config import config
from kpidebug.api.routes_users import router as users_router
from kpidebug.api.routes_projects import router as projects_router

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


@app.get("/api/health")
def health() -> dict:
    return {"status": "ok"}
