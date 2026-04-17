import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")


class Config:
    google_application_credentials: str = os.getenv(
        "GOOGLE_APPLICATION_CREDENTIALS",
        "./kpidebug/firebase-credentials.json",
    )
    frontend_url: str = os.getenv(
        "FRONTEND_URL", "http://localhost:3000"
    )
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))

    cache_enabled: bool = os.getenv(
        "CACHE_ENABLED", "false"
    ).lower() == "true"
    cache_backend: str = os.getenv(
        "CACHE_BACKEND", "memory"
    )
    cache_ttl_seconds: int = int(os.getenv(
        "CACHE_TTL_SECONDS", "300"
    ))

    store_backend: str = os.getenv(
        "STORE_BACKEND", "firestore"
    )
    database_url: str = os.getenv(
        "DATABASE_URL", ""
    )


config = Config()
