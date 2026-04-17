from unittest.mock import MagicMock

from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import DataSourceType


class TestPostgresDataSourceStore:
    def _make_store(self) -> tuple[PostgresDataSourceStore, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        store = PostgresDataSourceStore(pool_manager)
        return store, pool

    def _mock_connection(self, pool: MagicMock) -> MagicMock:
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_create_source(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        source = store.create_source("p1", "Stripe Prod", DataSourceType.STRIPE)

        assert source.id != ""
        assert source.name == "Stripe Prod"
        assert source.type == DataSourceType.STRIPE
        assert source.credentials == {}
        conn.execute.assert_called_once()

    def test_create_source_with_credentials(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        source = store.create_source(
            "p1", "Stripe", DataSourceType.STRIPE,
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
            ("s1", "p1", "Stripe", "stripe", {}),
            ("s2", "p1", "GA", "google_analytics", {}),
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
            "s1", "p1", "Stripe Updated", "stripe", {},
        )

        result = store.update_source("p1", "s1", {"name": "Stripe Updated"})

        assert result.name == "Stripe Updated"

    def test_ensure_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.ensure_tables()

        calls = conn.execute.call_args_list
        assert any("CREATE TABLE IF NOT EXISTS data_sources" in str(c) for c in calls)

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

        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("DELETE FROM data_sources" in c for c in calls)

    def test_ensure_tables_creates_cache_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.ensure_tables()

        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("cached_table_data" in c for c in calls)
        assert any("cached_table_meta" in c for c in calls)

    def test_set_cached_rows_and_get(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = ("2025-01-01T00:00:00Z",)
        conn.execute.return_value.fetchall.return_value = [
            ({"id": "1"},), ({"id": "2"},),
        ]

        store.set_cached_rows("s1", "charges", [{"id": "1"}, {"id": "2"}])

        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("INSERT INTO cached_table_data" in c for c in calls)
        assert any("INSERT INTO cached_table_meta" in c for c in calls)

    def test_get_cached_rows_returns_none_when_not_cached(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        result = store.get_cached_rows("s1", "charges")

        assert result is None

    def test_is_table_cached(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (1,)

        assert store.is_table_cached("s1", "charges") is True

    def test_is_table_not_cached(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert store.is_table_cached("s1", "charges") is False

    def test_clear_cached_table(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.clear_cached_table("s1", "charges")

        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("DELETE FROM cached_table_data" in c for c in calls)
        assert any("DELETE FROM cached_table_meta" in c for c in calls)

    def test_clear_cached_source(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.clear_cached_source("s1")

        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("DELETE FROM cached_table_data" in c for c in calls)
        assert any("DELETE FROM cached_table_meta" in c for c in calls)
