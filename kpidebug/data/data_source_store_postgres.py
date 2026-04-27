import json
import uuid

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.data.types import DataSource, DataSourceType, TableFilter, TableQuery, TableResult
from kpidebug.data.data_source_store import AbstractDataSourceStore


class PostgresDataSourceStore(AbstractDataSourceStore):
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
                    credentials JSONB NOT NULL DEFAULT '{}',
                    PRIMARY KEY (project_id, id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cached_table_data (
                    source_id TEXT NOT NULL,
                    table_key TEXT NOT NULL,
                    row_index INTEGER NOT NULL,
                    data JSONB NOT NULL,
                    PRIMARY KEY (source_id, table_key, row_index)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cached_table_meta (
                    source_id TEXT NOT NULL,
                    table_key TEXT NOT NULL,
                    synced_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (source_id, table_key)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_data_sources_project_id
                ON data_sources(project_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_cached_table_data_source_id
                ON cached_table_data(source_id)
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS cached_table_data CASCADE")
            conn.execute("DROP TABLE IF EXISTS cached_table_meta CASCADE")
            conn.execute("DROP TABLE IF EXISTS data_sources CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM cached_table_data")
            conn.execute("DELETE FROM cached_table_meta")
            conn.execute("DELETE FROM data_sources")

    def create_source(
        self, project_id: str, name: str,
        source_type: DataSourceType,
        credentials: dict[str, str] | None = None,
    ) -> DataSource:
        source_id = str(uuid.uuid4())
        creds = credentials or {}
        creds_json = json.dumps(creds)
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO data_sources (id, project_id, name, type, credentials) VALUES (%s, %s, %s, %s, %s)",
                (source_id, project_id, name, source_type.value, creds_json),
            )
        return DataSource(
            id=source_id, project_id=project_id,
            name=name, type=source_type, credentials=creds,
        )

    def get_source(self, project_id: str, source_id: str) -> DataSource | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT id, project_id, name, type, credentials FROM data_sources WHERE project_id = %s AND id = %s",
                (project_id, source_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_source(row)

    def list_sources(self, project_id: str) -> list[DataSource]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT id, project_id, name, type, credentials FROM data_sources WHERE project_id = %s",
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

    def query_cached_rows(self, source_id: str, table_key: str, query: TableQuery) -> TableResult | None:
        with self.pool.connection() as conn:
            meta = conn.execute(
                "SELECT synced_at FROM cached_table_meta WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            ).fetchone()
        if meta is None:
            return None

        where = "source_id = %s AND table_key = %s"
        params: list = [source_id, table_key]

        for f in query.filters:
            col_ref = f"data->>'{f.column}'"
            if f.operator == "eq":
                where += f" AND {col_ref} = %s"
                params.append(f.value)
            elif f.operator == "neq":
                where += f" AND {col_ref} != %s"
                params.append(f.value)
            elif f.operator == "contains":
                where += f" AND {col_ref} ILIKE %s"
                params.append(f"%{f.value}%")
            elif f.operator in ("gt", "gte", "lt", "lte"):
                sql_op = {"gt": ">", "gte": ">=", "lt": "<", "lte": "<="}[f.operator]
                try:
                    numeric_val = float(f.value)
                    where += f" AND ({col_ref})::numeric {sql_op} %s"
                    params.append(numeric_val)
                except ValueError:
                    where += f" AND {col_ref} {sql_op} %s"
                    params.append(f.value)

        with self.pool.connection() as conn:
            count_row = conn.execute(
                f"SELECT COUNT(*) FROM cached_table_data WHERE {where}",
                params,
            ).fetchone()
            total_count = count_row[0] if count_row else 0

        order = ""
        if query.sort_by:
            direction = "DESC" if query.sort_order == "desc" else "ASC"
            order = f" ORDER BY data->>'{query.sort_by}' {direction}"
        else:
            order = " ORDER BY row_index"

        data_params = list(params)
        data_params.extend([query.limit, query.offset])

        with self.pool.connection() as conn:
            rows = conn.execute(
                f"SELECT data FROM cached_table_data WHERE {where}{order} LIMIT %s OFFSET %s",
                data_params,
            ).fetchall()

        result_rows = [r[0] if isinstance(r[0], dict) else json.loads(r[0]) for r in rows]
        return TableResult(rows=result_rows, total_count=total_count)

    def get_cached_rows(self, source_id: str, table_key: str) -> list[dict] | None:
        with self.pool.connection() as conn:
            meta = conn.execute(
                "SELECT synced_at FROM cached_table_meta WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            ).fetchone()
        if meta is None:
            return None
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT data FROM cached_table_data WHERE source_id = %s AND table_key = %s ORDER BY row_index",
                (source_id, table_key),
            ).fetchall()
        return [r[0] if isinstance(r[0], dict) else json.loads(r[0]) for r in rows]

    def set_cached_rows(self, source_id: str, table_key: str, rows: list[dict], synced_at: str = "") -> None:
        if not synced_at:
            from datetime import datetime, timezone
            synced_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM cached_table_data WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            )
            for i, row in enumerate(rows):
                conn.execute(
                    "INSERT INTO cached_table_data (source_id, table_key, row_index, data) VALUES (%s, %s, %s, %s)",
                    (source_id, table_key, i, json.dumps(row)),
                )
            conn.execute(
                """
                INSERT INTO cached_table_meta (source_id, table_key, synced_at)
                VALUES (%s, %s, %s)
                ON CONFLICT (source_id, table_key) DO UPDATE SET synced_at = EXCLUDED.synced_at
                """,
                (source_id, table_key, synced_at),
            )

    def is_table_cached(self, source_id: str, table_key: str) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT 1 FROM cached_table_meta WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            ).fetchone()
        return row is not None

    def clear_cached_table(self, source_id: str, table_key: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM cached_table_data WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            )
            conn.execute(
                "DELETE FROM cached_table_meta WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            )

    def clear_cached_source(self, source_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM cached_table_data WHERE source_id = %s",
                (source_id,),
            )
            conn.execute(
                "DELETE FROM cached_table_meta WHERE source_id = %s",
                (source_id,),
            )

    def _row_to_source(self, row: tuple) -> DataSource:
        raw_creds = row[4] if isinstance(row[4], dict) else json.loads(row[4]) if isinstance(row[4], str) else row[4]
        return DataSource(
            id=row[0], project_id=row[1],
            name=row[2], type=DataSourceType(row[3]),
            credentials=raw_creds or {},
        )
