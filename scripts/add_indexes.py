"""Add missing indexes to the live database.

Safe to run repeatedly — all statements use IF NOT EXISTS.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from kpidebug.common.db import ConnectionPoolManager

INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_project_members_user_id ON project_members(user_id)",
    "CREATE INDEX IF NOT EXISTS idx_users_email ON users(email)",
    "CREATE INDEX IF NOT EXISTS idx_data_sources_project_id ON data_sources(project_id)",
    "CREATE INDEX IF NOT EXISTS idx_cached_table_data_source_id ON cached_table_data(source_id)",
    "CREATE INDEX IF NOT EXISTS idx_project_artifacts_project_id ON project_artifacts(project_id)",
    "CREATE INDEX IF NOT EXISTS idx_dashboard_metrics_project_id ON dashboard_metrics(project_id)",
]


def main() -> None:
    pool_manager = ConnectionPoolManager()
    try:
        with pool_manager.pool().connection() as conn:
            for stmt in INDEXES:
                conn.execute(stmt)
                name = stmt.split("EXISTS")[1].strip().split(" ")[0]
                print(f"  {name}")
        print(f"Done. {len(INDEXES)} indexes ensured.")
    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
