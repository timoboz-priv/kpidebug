import uuid
from datetime import datetime, timezone

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.metrics.types import DashboardMetric


class PostgresDashboardStore:
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dashboard_metrics (
                    id TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    source_id TEXT NOT NULL,
                    metric_key TEXT NOT NULL,
                    position INTEGER NOT NULL DEFAULT 0,
                    added_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (project_id, id),
                    UNIQUE (project_id, source_id, metric_key)
                )
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS dashboard_metrics CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM dashboard_metrics")

    def add_metric(
        self, project_id: str, source_id: str, metric_key: str,
    ) -> DashboardMetric:
        metric_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with self.pool.connection() as conn:
            max_pos = conn.execute(
                "SELECT COALESCE(MAX(position), -1) FROM dashboard_metrics WHERE project_id = %s",
                (project_id,),
            ).fetchone()[0]

            conn.execute(
                """
                INSERT INTO dashboard_metrics
                    (id, project_id, source_id, metric_key, position, added_at)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (metric_id, project_id, source_id, metric_key, max_pos + 1, now),
            )

        return DashboardMetric(
            id=metric_id,
            project_id=project_id,
            source_id=source_id,
            metric_key=metric_key,
            position=max_pos + 1,
            added_at=now,
        )

    def remove_metric(self, project_id: str, dashboard_metric_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM dashboard_metrics WHERE project_id = %s AND id = %s",
                (project_id, dashboard_metric_id),
            )

    def list_metrics(self, project_id: str) -> list[DashboardMetric]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, project_id, source_id, metric_key, position, added_at
                FROM dashboard_metrics
                WHERE project_id = %s
                ORDER BY position
                """,
                (project_id,),
            ).fetchall()
        return [self._row_to_metric(r) for r in rows]

    def _row_to_metric(self, row: tuple) -> DashboardMetric:
        return DashboardMetric(
            id=row[0],
            project_id=row[1],
            source_id=row[2],
            metric_key=row[3],
            position=int(row[4]),
            added_at=row[5],
        )
