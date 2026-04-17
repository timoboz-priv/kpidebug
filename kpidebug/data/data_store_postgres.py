import json
import uuid

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.data.types import (
    DataSource,
    DataSourceType,
    Dimension,
    DimensionType,
)
from kpidebug.data.data_store import AbstractDataStore


class PostgresDataStore(AbstractDataStore):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS data_sources (
                    id TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    type TEXT NOT NULL DEFAULT 'custom',
                    dimensions JSONB NOT NULL DEFAULT '[]',
                    credentials JSONB NOT NULL DEFAULT '{}',
                    PRIMARY KEY (project_id, id)
                )
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS data_sources CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM data_sources")

    def create_source(
        self, project_id: str, name: str,
        source_type: DataSourceType, dimensions: list[Dimension],
        credentials: dict[str, str] | None = None,
    ) -> DataSource:
        source_id = str(uuid.uuid4())
        dimensions_json = json.dumps([
            {"name": d.name, "type": d.type.value}
            for d in dimensions
        ])
        creds = credentials or {}
        creds_json = json.dumps(creds)
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO data_sources (id, project_id, name, type, dimensions, credentials) VALUES (%s, %s, %s, %s, %s, %s)",
                (source_id, project_id, name, source_type.value, dimensions_json, creds_json),
            )
        return DataSource(
            id=source_id, project_id=project_id,
            name=name, type=source_type, dimensions=dimensions,
            credentials=creds,
        )

    def get_source(self, project_id: str, source_id: str) -> DataSource | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT id, project_id, name, type, dimensions, credentials FROM data_sources WHERE project_id = %s AND id = %s",
                (project_id, source_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_source(row)

    def list_sources(self, project_id: str) -> list[DataSource]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT id, project_id, name, type, dimensions, credentials FROM data_sources WHERE project_id = %s",
                (project_id,),
            ).fetchall()
        return [self._row_to_source(r) for r in rows]

    def delete_source(self, project_id: str, source_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM data_sources WHERE project_id = %s AND id = %s",
                (project_id, source_id),
            )

    def update_source(self, project_id: str, source_id: str, updates: dict) -> DataSource:
        if not updates:
            return self.get_source(project_id, source_id)
        set_clauses = ", ".join(f"{key} = %s" for key in updates)
        values = list(updates.values()) + [project_id, source_id]
        with self.pool.connection() as conn:
            conn.execute(
                f"UPDATE data_sources SET {set_clauses} WHERE project_id = %s AND id = %s",
                values,
            )
        return self.get_source(project_id, source_id)

    def _row_to_source(self, row: tuple) -> DataSource:
        raw_dims = row[4] if isinstance(row[4], list) else json.loads(row[4]) if isinstance(row[4], str) else row[4]
        dimensions = [
            Dimension(name=d["name"], type=DimensionType(d["type"]))
            for d in raw_dims
        ]
        raw_creds = row[5] if isinstance(row[5], dict) else json.loads(row[5]) if isinstance(row[5], str) else row[5]
        return DataSource(
            id=row[0], project_id=row[1],
            name=row[2], type=DataSourceType(row[3]),
            dimensions=dimensions,
            credentials=raw_creds or {},
        )
