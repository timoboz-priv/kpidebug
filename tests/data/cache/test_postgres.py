from unittest.mock import MagicMock, call

from kpidebug.data.cache.postgres import PostgresTableCache


class TestPostgresTableCache:
    def _make_cache(self) -> tuple[PostgresTableCache, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        cache = PostgresTableCache(pool_manager)
        return cache, pool

    def _mock_connection(self, pool: MagicMock) -> MagicMock:
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_is_cached_false_when_no_meta(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert cache.is_cached("s1", "charges") is False

    def test_is_cached_true(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (True,)

        assert cache.is_cached("s1", "charges") is True

    def test_set_rows_inserts_rows_and_meta(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)

        rows = [{"id": "1", "amount": 100}, {"id": "2", "amount": 200}]
        cache.set_rows("s1", "charges", rows)

        calls = conn.execute.call_args_list
        assert any("DELETE FROM table_cache_rows" in str(c) for c in calls)
        assert any("INSERT INTO table_cache_rows" in str(c) for c in calls)
        assert any("INSERT INTO table_cache_meta" in str(c) for c in calls)

    def test_get_rows_returns_none_when_not_cached(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert cache.get_rows("s1", "charges") is None

    def test_clear_table(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)

        cache.clear_table("s1", "charges")

        calls = conn.execute.call_args_list
        assert any("DELETE FROM table_cache_rows" in str(c) for c in calls)
        assert any("cached = FALSE" in str(c) for c in calls)

    def test_ensure_tables(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)

        cache.ensure_tables()

        calls = conn.execute.call_args_list
        assert any("CREATE TABLE IF NOT EXISTS table_cache_rows" in str(c) for c in calls)
        assert any("CREATE TABLE IF NOT EXISTS table_cache_meta" in str(c) for c in calls)

    def test_drop_tables(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)

        cache.drop_tables()

        calls = conn.execute.call_args_list
        assert any("DROP TABLE IF EXISTS table_cache_rows" in str(c) for c in calls)
        assert any("DROP TABLE IF EXISTS table_cache_meta" in str(c) for c in calls)

    def test_clean(self):
        cache, pool = self._make_cache()
        conn = self._mock_connection(pool)

        cache.clean()

        calls = conn.execute.call_args_list
        assert any("DELETE FROM table_cache_rows" in str(c) for c in calls)
        assert any("DELETE FROM table_cache_meta" in str(c) for c in calls)
