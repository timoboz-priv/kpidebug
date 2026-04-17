from unittest.mock import MagicMock

from kpidebug.data.data_store_postgres import PostgresDataStore
from kpidebug.data.types import DataSourceType, Dimension, DimensionType


class TestPostgresDataStore:
    def _make_store(self) -> tuple[PostgresDataStore, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        store = PostgresDataStore(pool_manager)
        return store, pool

    def _mock_connection(self, pool: MagicMock) -> MagicMock:
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_create_source(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        dims = [
            Dimension(name="time", type=DimensionType.TEMPORAL),
            Dimension(name="country", type=DimensionType.CATEGORICAL),
        ]

        source = store.create_source("p1", "Stripe Prod", DataSourceType.STRIPE, dims)

        assert source.id != ""
        assert source.name == "Stripe Prod"
        assert source.type == DataSourceType.STRIPE
        assert len(source.dimensions) == 2
        assert source.credentials == {}
        conn.execute.assert_called_once()

    def test_create_source_with_credentials(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        source = store.create_source(
            "p1", "Stripe", DataSourceType.STRIPE, [],
            credentials={"api_key": "sk_test"},
        )

        assert source.credentials == {"api_key": "sk_test"}
        args = conn.execute.call_args
        assert "INSERT INTO data_sources" in args[0][0]

    def test_get_source_returns_source(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (
            "s1", "p1", "Stripe Prod", "stripe",
            [{"name": "time", "type": "temporal"}],
            {"api_key": "sk_test"},
        )

        source = store.get_source("p1", "s1")

        assert source is not None
        assert source.id == "s1"
        assert source.type == DataSourceType.STRIPE
        assert source.credentials == {"api_key": "sk_test"}

    def test_get_source_returns_none(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert store.get_source("p1", "s1") is None

    def test_list_sources(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("s1", "p1", "Stripe", "stripe", [], {}),
            ("s2", "p1", "GA", "google_analytics", [], {}),
        ]

        sources = store.list_sources("p1")

        assert len(sources) == 2

    def test_delete_source(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.delete_source("p1", "s1")

        args = conn.execute.call_args
        assert "DELETE FROM data_sources" in args[0][0]

    def test_update_source(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (
            "s1", "p1", "Stripe Updated", "stripe", [], {},
        )

        result = store.update_source("p1", "s1", {"name": "Stripe Updated"})

        assert result.name == "Stripe Updated"

    def test_ensure_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.ensure_tables()

        calls = conn.execute.call_args_list
        assert any("CREATE TABLE IF NOT EXISTS data_sources" in str(c) for c in calls)
        assert not any("data_source_credentials" in str(c) for c in calls)

    def test_drop_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.drop_tables()

        args = conn.execute.call_args
        assert "DROP TABLE IF EXISTS data_sources" in args[0][0]

    def test_clean(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.clean()

        args = conn.execute.call_args
        assert "DELETE FROM data_sources" in args[0][0]
