import json
from unittest.mock import MagicMock

from kpidebug.data.types import DataSourceType, DimensionValue
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore
from kpidebug.metrics.types import (
    MetricDataType,
    MetricDefinition,
    MetricResult,
    MetricSource,
    SourceFilter,
)


class TestPostgresMetricStore:
    def _make_store(self) -> tuple[PostgresMetricStore, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        store = PostgresMetricStore(pool_manager)
        return store, pool

    def _mock_connection(self, pool: MagicMock) -> MagicMock:
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_create_definition(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        definition = MetricDefinition(
            project_id="p1",
            name="Revenue",
            description="Monthly revenue",
            data_type=MetricDataType.CURRENCY,
            source=MetricSource.DATA_SOURCE,
            source_filters=[SourceFilter(source_type=DataSourceType.STRIPE, fields=["amount"])],
            dimensions=["time"],
        )

        result = store.create_definition(definition)

        assert result.id != ""
        assert result.created_at != ""
        assert result.updated_at != ""
        conn.execute.assert_called_once()
        args = conn.execute.call_args
        assert "INSERT INTO metric_definitions" in args[0][0]

    def test_get_definition_returns_definition(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (
            "m1", "p1", "Revenue", "Monthly revenue",
            "currency", "data_source", "", "",
            [{"source_type": "stripe", "fields": ["amount"]}],
            ["time"],
            "2025-01-01T00:00:00Z", "2025-01-01T00:00:00Z",
        )

        defn = store.get_definition("p1", "m1")

        assert defn is not None
        assert defn.id == "m1"
        assert defn.data_type == MetricDataType.CURRENCY
        assert len(defn.source_filters) == 1

    def test_get_definition_returns_none(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert store.get_definition("p1", "m1") is None

    def test_list_definitions(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("m1", "p1", "Revenue", "", "number", "builtin", "", "", [], [], "", ""),
            ("m2", "p1", "Users", "", "number", "builtin", "", "", [], [], "", ""),
        ]

        defs = store.list_definitions("p1")

        assert len(defs) == 2
        assert defs[0].id == "m1"

    def test_delete_definition(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.delete_definition("p1", "m1")

        args = conn.execute.call_args
        assert "DELETE FROM metric_definitions" in args[0][0]

    def test_store_results(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        results = [
            MetricResult(
                project_id="p1", metric_id="m1", value=100.0,
                dimension_values=[DimensionValue(dimension="time", value="2025-01")],
                computed_at="2025-01-01T00:00:00Z",
                period_start="2025-01-01", period_end="2025-01-31",
            ),
        ]

        store.store_results(results)

        args = conn.execute.call_args
        assert "INSERT INTO metric_results" in args[0][0]

    def test_store_results_empty(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.store_results([])

        conn.execute.assert_not_called()

    def test_get_results(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("r1", "p1", "m1", 100.0,
             [{"dimension": "time", "value": "2025-01"}],
             "2025-01-01T00:00:00Z", "2025-01-01", "2025-01-31"),
        ]

        results = store.get_results("p1", "m1")

        assert len(results) == 1
        assert results[0].value == 100.0
        assert results[0].dimension_values[0].dimension == "time"

    def test_get_results_with_time_filters(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = []

        store.get_results("p1", "m1", start_time="2025-01-01", end_time="2025-12-31")

        query = conn.execute.call_args[0][0]
        assert "computed_at >= %s" in query
        assert "computed_at <= %s" in query

    def test_get_latest_result(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (
            "r1", "p1", "m1", 200.0, [], "2025-06-01T00:00:00Z", "2025-06-01", "2025-06-30",
        )

        result = store.get_latest_result("p1", "m1")

        assert result is not None
        assert result.value == 200.0
        query = conn.execute.call_args[0][0]
        assert "ORDER BY computed_at DESC" in query

    def test_get_latest_result_none(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert store.get_latest_result("p1", "m1") is None

    def test_ensure_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.ensure_tables()

        calls = conn.execute.call_args_list
        assert any("CREATE TABLE IF NOT EXISTS metric_definitions" in str(c) for c in calls)
        assert any("CREATE TABLE IF NOT EXISTS metric_results" in str(c) for c in calls)
        assert any("CREATE INDEX IF NOT EXISTS idx_metric_results_metric" in str(c) for c in calls)
        assert any("CREATE INDEX IF NOT EXISTS idx_metric_results_computed_at" in str(c) for c in calls)

    def test_drop_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.drop_tables()

        calls = conn.execute.call_args_list
        assert any("DROP TABLE IF EXISTS metric_results" in str(c) for c in calls)
        assert any("DROP TABLE IF EXISTS metric_definitions" in str(c) for c in calls)

    def test_clean(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.clean()

        calls = conn.execute.call_args_list
        assert any("DELETE FROM metric_results" in str(c) for c in calls)
        assert any("DELETE FROM metric_definitions" in str(c) for c in calls)
