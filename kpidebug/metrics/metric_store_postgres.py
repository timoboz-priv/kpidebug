import json
import uuid
from datetime import datetime, timezone

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.metrics.types import (
    Aggregation,
    DimensionValue,
    MetricDataType,
    MetricDefinition,
    MetricDefinitionUpdate,
    MetricResult,
    MetricSource,
    SourceFilter,
)
from kpidebug.metrics.metric_store import AbstractMetricStore


class PostgresMetricStore(AbstractMetricStore):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_definitions (
                    id TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    name TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    data_type TEXT NOT NULL DEFAULT 'number',
                    source TEXT NOT NULL DEFAULT 'builtin',
                    source_id TEXT NOT NULL DEFAULT '',
                    table_name TEXT NOT NULL DEFAULT '',
                    builtin_key TEXT NOT NULL DEFAULT '',
                    value_field TEXT NOT NULL DEFAULT '',
                    aggregation TEXT NOT NULL DEFAULT 'sum',
                    computation TEXT NOT NULL DEFAULT '',
                    source_filters JSONB NOT NULL DEFAULT '[]',
                    dimensions JSONB NOT NULL DEFAULT '[]',
                    created_at TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (project_id, id)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS metric_results (
                    id TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    metric_id TEXT NOT NULL,
                    value DOUBLE PRECISION NOT NULL DEFAULT 0.0,
                    dimension_values JSONB NOT NULL DEFAULT '[]',
                    computed_at TEXT NOT NULL DEFAULT '',
                    period_start TEXT NOT NULL DEFAULT '',
                    period_end TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (project_id, id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metric_results_metric
                    ON metric_results(project_id, metric_id)
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metric_results_computed_at
                    ON metric_results(project_id, metric_id, computed_at)
            """)
            for col, default in [
                ("source_id", "''"),
                ("table_name", "''"),
                ("value_field", "''"),
                ("aggregation", "'sum'"),
            ]:
                conn.execute(f"""
                    ALTER TABLE metric_definitions
                    ADD COLUMN IF NOT EXISTS {col} TEXT NOT NULL DEFAULT {default}
                """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_metric_definitions_source
                    ON metric_definitions(project_id, source_id)
            """)
            conn.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_metric_definitions_builtin
                    ON metric_definitions(project_id, source_id, builtin_key)
                    WHERE builtin_key != ''
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS metric_results CASCADE")
            conn.execute("DROP TABLE IF EXISTS metric_definitions CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM metric_results")
            conn.execute("DELETE FROM metric_definitions")

    _COLUMNS = (
        "id, project_id, name, description, data_type, source, "
        "source_id, table_name, builtin_key, value_field, aggregation, "
        "computation, source_filters, dimensions, created_at, updated_at"
    )

    def create_definition(self, definition: MetricDefinition) -> MetricDefinition:
        metric_id = definition.id or str(uuid.uuid4())
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        source_filters_json = json.dumps([
            {"source_type": sf.source_type.value, "fields": sf.fields}
            for sf in definition.source_filters
        ])
        dimensions_json = json.dumps(definition.dimensions)

        with self.pool.connection() as conn:
            conn.execute(
                f"""
                INSERT INTO metric_definitions ({self._COLUMNS})
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    metric_id, definition.project_id, definition.name,
                    definition.description, definition.data_type.value,
                    definition.source.value, definition.source_id,
                    definition.table, definition.builtin_key,
                    definition.value_field, definition.aggregation,
                    definition.computation, source_filters_json,
                    dimensions_json, now, now,
                ),
            )

        definition.id = metric_id
        definition.created_at = now
        definition.updated_at = now
        return definition

    def get_definition(self, project_id: str, metric_id: str) -> MetricDefinition | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                f"SELECT {self._COLUMNS} FROM metric_definitions "
                "WHERE project_id = %s AND id = %s",
                (project_id, metric_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_definition(row)

    def list_definitions(self, project_id: str) -> list[MetricDefinition]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                f"SELECT {self._COLUMNS} FROM metric_definitions "
                "WHERE project_id = %s",
                (project_id,),
            ).fetchall()
        return [self._row_to_definition(r) for r in rows]

    def list_for_source(self, project_id: str, source_id: str) -> list[MetricDefinition]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                f"SELECT {self._COLUMNS} FROM metric_definitions "
                "WHERE project_id = %s AND source_id = %s",
                (project_id, source_id),
            ).fetchall()
        return [self._row_to_definition(r) for r in rows]

    def get_by_builtin_key(
        self, project_id: str, source_id: str, builtin_key: str,
    ) -> MetricDefinition | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                f"SELECT {self._COLUMNS} FROM metric_definitions "
                "WHERE project_id = %s AND source_id = %s AND builtin_key = %s",
                (project_id, source_id, builtin_key),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_definition(row)

    def update_definition(
        self, project_id: str, metric_id: str, updates: MetricDefinitionUpdate,
    ) -> MetricDefinition:
        fields = updates.to_db_fields()
        fields["updated_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        set_clauses = ", ".join(f"{key} = %s" for key in fields)
        values = list(fields.values()) + [project_id, metric_id]
        with self.pool.connection() as conn:
            conn.execute(
                f"UPDATE metric_definitions SET {set_clauses} WHERE project_id = %s AND id = %s",
                values,
            )
        return self.get_definition(project_id, metric_id)

    def delete_definition(self, project_id: str, metric_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM metric_definitions WHERE project_id = %s AND id = %s",
                (project_id, metric_id),
            )

    def store_results(self, results: list[MetricResult]) -> None:
        if not results:
            return
        with self.pool.connection() as conn:
            for result in results:
                result_id = result.id or str(uuid.uuid4())
                dimension_values_json = json.dumps([
                    {"dimension": dv.dimension, "value": dv.value}
                    for dv in result.dimension_values
                ])
                conn.execute(
                    """
                    INSERT INTO metric_results
                        (id, project_id, metric_id, value, dimension_values,
                         computed_at, period_start, period_end)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        result_id, result.project_id, result.metric_id,
                        result.value, dimension_values_json,
                        result.computed_at, result.period_start, result.period_end,
                    ),
                )

    def get_results(
        self,
        project_id: str,
        metric_id: str,
        start_time: str | None = None,
        end_time: str | None = None,
    ) -> list[MetricResult]:
        query = """
            SELECT id, project_id, metric_id, value, dimension_values,
                   computed_at, period_start, period_end
            FROM metric_results
            WHERE project_id = %s AND metric_id = %s
        """
        params: list = [project_id, metric_id]

        if start_time is not None:
            query += " AND computed_at >= %s"
            params.append(start_time)
        if end_time is not None:
            query += " AND computed_at <= %s"
            params.append(end_time)

        with self.pool.connection() as conn:
            rows = conn.execute(query, params).fetchall()
        return [self._row_to_result(r) for r in rows]

    def get_latest_result(self, project_id: str, metric_id: str) -> MetricResult | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                """
                SELECT id, project_id, metric_id, value, dimension_values,
                       computed_at, period_start, period_end
                FROM metric_results
                WHERE project_id = %s AND metric_id = %s
                ORDER BY computed_at DESC
                LIMIT 1
                """,
                (project_id, metric_id),
            ).fetchone()
        if row is None:
            return None
        return self._row_to_result(row)

    def _row_to_definition(self, row: tuple) -> MetricDefinition:
        raw_filters = row[12] if isinstance(row[12], list) else json.loads(row[12]) if isinstance(row[12], str) else row[12]
        source_filters = []
        from kpidebug.data.types import DataSourceType
        for sf in raw_filters:
            source_filters.append(SourceFilter(
                source_type=DataSourceType(sf["source_type"]),
                fields=sf.get("fields", []),
            ))
        raw_dims = row[13] if isinstance(row[13], list) else json.loads(row[13]) if isinstance(row[13], str) else row[13]
        return MetricDefinition(
            id=row[0],
            project_id=row[1],
            name=row[2],
            description=row[3],
            data_type=MetricDataType(row[4]),
            source=MetricSource(row[5]),
            source_id=row[6],
            table=row[7],
            builtin_key=row[8],
            value_field=row[9],
            aggregation=Aggregation(row[10]) if row[10] else Aggregation.SUM,
            computation=row[11],
            source_filters=source_filters,
            dimensions=raw_dims,
            created_at=row[14],
            updated_at=row[15],
        )

    def _row_to_result(self, row: tuple) -> MetricResult:
        raw_dvs = row[4] if isinstance(row[4], list) else json.loads(row[4]) if isinstance(row[4], str) else row[4]
        dimension_values = [
            DimensionValue(dimension=dv["dimension"], value=dv["value"])
            for dv in raw_dvs
        ]
        return MetricResult(
            id=row[0],
            project_id=row[1],
            metric_id=row[2],
            value=float(row[3]),
            dimension_values=dimension_values,
            computed_at=row[5],
            period_start=row[6],
            period_end=row[7],
        )
