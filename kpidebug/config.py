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

    database_url: str = os.getenv(
        "DATABASE_URL", ""
    )


config = Config()
