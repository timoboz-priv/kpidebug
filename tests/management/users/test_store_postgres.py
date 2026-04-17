from unittest.mock import MagicMock, patch

from kpidebug.management.user_store_postgres import PostgresUserStore
from kpidebug.management.types import User


class TestPostgresUserStore:
    def _make_store(self) -> tuple[PostgresUserStore, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        store = PostgresUserStore(pool_manager)
        return store, pool

    def _mock_connection(self, pool: MagicMock) -> MagicMock:
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_get_returns_user_when_exists(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = ("u1", "Alice", "alice@test.com", "")

        user = store.get("u1")

        assert user is not None
        assert user.id == "u1"
        assert user.name == "Alice"
        assert user.email == "alice@test.com"

    def test_get_returns_none_when_not_exists(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        user = store.get("u1")

        assert user is None

    def test_create_inserts_user(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        user = User(id="u1", name="Alice", email="alice@test.com", avatar_url="")

        result = store.create(user)

        conn.execute.assert_called_once()
        args = conn.execute.call_args
        assert "INSERT INTO users" in args[0][0]
        assert args[0][1] == ("u1", "Alice", "alice@test.com", "")
        assert result.id == "u1"

    def test_update_sets_fields(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = ("u1", "Bob", "bob@test.com", "")

        store.update("u1", {"name": "Bob"})

        first_call = conn.execute.call_args_list[0]
        assert "UPDATE users SET" in first_call[0][0]

    def test_get_or_create_returns_existing(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = ("u1", "Alice", "alice@test.com", "")

        user = store.get_or_create("u1", "alice@test.com", "Alice", None)

        assert user.name == "Alice"

    def test_get_or_create_creates_when_missing(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        user = store.get_or_create("u1", "alice@test.com", "Alice", None)

        assert user.id == "u1"
        assert user.name == "Alice"
        assert conn.execute.call_count >= 2

    def test_ensure_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.ensure_tables()

        args = conn.execute.call_args
        assert "CREATE TABLE IF NOT EXISTS users" in args[0][0]

    def test_drop_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.drop_tables()

        args = conn.execute.call_args
        assert "DROP TABLE IF EXISTS users" in args[0][0]

    def test_clean(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.clean()

        args = conn.execute.call_args
        assert "DELETE FROM users" in args[0][0]
