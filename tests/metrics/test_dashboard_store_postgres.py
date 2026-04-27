from unittest.mock import MagicMock

from datetime import date

from kpidebug.metrics.dashboard_store_postgres import PostgresDashboardStore
from kpidebug.metrics.types import MetricSnapshot


class TestPostgresDashboardStore:
    def _make_store(self) -> tuple[PostgresDashboardStore, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        store = PostgresDashboardStore(pool_manager)
        return store, pool

    def _mock_connection(self, pool: MagicMock) -> MagicMock:
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_ensure_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.ensure_tables()

        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("CREATE TABLE IF NOT EXISTS dashboard_metrics" in c for c in calls)
        assert any("metric_id" in c for c in calls)
        assert any("PRIMARY KEY (project_id, id)" in c for c in calls)

    def test_add_metric(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (2,)

        result = store.add_metric("p1", "metric-uuid-123")

        assert result.project_id == "p1"
        assert result.metric_id == "metric-uuid-123"
        assert result.id != ""
        assert result.position == 3
        assert result.added_at != ""
        assert conn.execute.call_count == 2

    def test_add_metric_first_position(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (-1,)

        result = store.add_metric("p1", "metric-uuid-123")

        assert result.position == 0

    def test_remove_metric(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.remove_metric("p1", "dm-123")

        conn.execute.assert_called_once()
        args = conn.execute.call_args
        assert "DELETE FROM dashboard_metrics" in args[0][0]
        assert args[0][1] == ("p1", "dm-123")

    def test_list_metrics(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("id1", "p1", "metric-1", 0, "2026-01-01T00:00:00Z", None),
            ("id2", "p1", "metric-2", 1, "2026-01-01T00:00:00Z", None),
        ]

        results = store.list_metrics("p1")

        assert len(results) == 2
        assert results[0].id == "id1"
        assert results[0].metric_id == "metric-1"
        assert results[0].position == 0
        assert results[0].snapshot is None
        assert results[1].id == "id2"
        assert results[1].metric_id == "metric-2"
        assert results[1].position == 1

    def test_list_metrics_empty(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = []

        results = store.list_metrics("p1")

        assert results == []

    def test_list_metrics_ordered_by_position(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = []

        store.list_metrics("p1")

        sql = conn.execute.call_args[0][0]
        assert "ORDER BY position" in sql

    def test_list_metrics_with_snapshot(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("id1", "p1", "metric-1", 0, "2026-01-01T00:00:00Z",
             {"date": "2026-01-15", "values": [10, 20, 30, 42]}),
        ]

        results = store.list_metrics("p1")

        assert results[0].snapshot is not None
        assert results[0].snapshot.value == 42.0
        assert results[0].snapshot.values == [10, 20, 30, 42]
        assert results[0].snapshot.date == date(2026, 1, 15)

    def test_store_snapshot(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        snapshot = MetricSnapshot(
            metric_id="m1",
            project_id="p1",
            date=date(2026, 1, 15),
            values=[10, 20, 30, 42],
        )
        store.store_snapshot("p1", "m1", snapshot)

        sql = conn.execute.call_args[0][0]
        assert "UPDATE dashboard_metrics SET snapshot" in sql
        params = conn.execute.call_args[0][1]
        assert params[1] == "p1"
        assert params[2] == "m1"
