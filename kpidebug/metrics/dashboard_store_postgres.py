import json
import uuid
from datetime import datetime, timezone

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.types import DashboardMetric, MetricSnapshot


class PostgresDashboardStore(AbstractDashboardStore):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dashboard_metrics (
                    id TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    metric_id TEXT NOT NULL,
                    position INTEGER NOT NULL DEFAULT 0,
                    added_at TEXT NOT NULL DEFAULT '',
                    snapshot JSONB DEFAULT NULL,
                    PRIMARY KEY (project_id, id),
                    UNIQUE (project_id, metric_id)
                )
            """)
            conn.execute("""
                ALTER TABLE dashboard_metrics ADD COLUMN IF NOT EXISTS snapshot JSONB DEFAULT NULL
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_dashboard_metrics_project_id
                ON dashboard_metrics(project_id)
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS dashboard_metrics CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM dashboard_metrics")

    def add_metric(
        self, project_id: str, metric_id: str,
    ) -> DashboardMetric:
        dm_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc)
        now_str = now.strftime("%Y-%m-%dT%H:%M:%SZ")

        with self.pool.connection() as conn:
            max_pos = conn.execute(
                "SELECT COALESCE(MAX(position), -1) FROM dashboard_metrics WHERE project_id = %s",
                (project_id,),
            ).fetchone()[0]

            conn.execute(
                """
                INSERT INTO dashboard_metrics
                    (id, project_id, metric_id, position, added_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (dm_id, project_id, metric_id, max_pos + 1, now_str),
            )

        return DashboardMetric(
            id=dm_id,
            project_id=project_id,
            metric_id=metric_id,
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
                SELECT id, project_id, metric_id, position, added_at, snapshot
                FROM dashboard_metrics
                WHERE project_id = %s
                ORDER BY position
                """,
                (project_id,),
            ).fetchall()
        return [self._row_to_metric(r) for r in rows]

    def store_snapshot(self, project_id: str, metric_id: str, snapshot: MetricSnapshot) -> None:
        snapshot_json = json.dumps({
            "date": snapshot.date.isoformat(),
            "values": snapshot.values,
        })
        with self.pool.connection() as conn:
            conn.execute(
                "UPDATE dashboard_metrics SET snapshot = %s WHERE project_id = %s AND metric_id = %s",
                (snapshot_json, project_id, metric_id),
            )

    def _row_to_metric(self, row: tuple) -> DashboardMetric:
        snapshot = None
        raw_snapshot = row[5] if len(row) > 5 else None
        if raw_snapshot is not None:
            data = raw_snapshot if isinstance(raw_snapshot, dict) else json.loads(raw_snapshot)
            from datetime import date as date_type
            date_str = data.get("date", "")
            snapshot_date = date_type.fromisoformat(date_str) if date_str else date_type.today()
            snapshot = MetricSnapshot(
                metric_id=row[2],
                project_id=row[1],
                date=snapshot_date,
                values=[float(v) for v in data.get("values", [])],
            )

        added_at_str = row[4] or ""
        added_at = (
            datetime.fromisoformat(added_at_str.replace("Z", "+00:00"))
            if added_at_str else datetime.now(timezone.utc)
        )

        return DashboardMetric(
            id=row[0],
            project_id=row[1],
            metric_id=row[2],
            position=int(row[3]),
            added_at=added_at,
            snapshot=snapshot,
        )
