from unittest.mock import MagicMock, call

from kpidebug.management.project_store_postgres import PostgresProjectStore
from kpidebug.management.types import Role


class TestPostgresProjectStore:
    def _make_store(self) -> tuple[PostgresProjectStore, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        store = PostgresProjectStore(pool_manager)
        return store, pool

    def _mock_connection(self, pool: MagicMock) -> MagicMock:
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return conn

    def test_get_returns_project(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = ("p1", "My Project", "desc")

        project = store.get("p1")

        assert project is not None
        assert project.id == "p1"
        assert project.name == "My Project"

    def test_get_returns_none(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert store.get("p1") is None

    def test_create_inserts_project_and_adds_admin(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        project = store.create("My Project", "desc", "u1", "Alice", "alice@test.com")

        assert project.name == "My Project"
        assert project.id != ""
        calls = conn.execute.call_args_list
        assert any("INSERT INTO projects" in str(c) for c in calls)
        assert any("INSERT INTO project_members" in str(c) for c in calls)

    def test_delete_removes_project(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.delete("p1")

        args = conn.execute.call_args
        assert "DELETE FROM projects" in args[0][0]

    def test_list_for_user(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("p1", "Project 1", "desc1"),
            ("p2", "Project 2", "desc2"),
        ]

        projects = store.list_for_user("u1")

        assert len(projects) == 2
        assert projects[0].id == "p1"
        assert projects[1].name == "Project 2"

    def test_get_members(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("u1", "admin", "Alice", "alice@test.com"),
        ]

        members = store.get_members("p1")

        assert len(members) == 1
        assert members[0].role == Role.ADMIN

    def test_add_member_upserts(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        member = store.add_member("p1", "u2", Role.EDIT, "Bob", "bob@test.com")

        assert member.user_id == "u2"
        assert member.role == Role.EDIT
        args = conn.execute.call_args
        assert "ON CONFLICT" in args[0][0]

    def test_get_member_returns_member(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = ("u1", "admin", "Alice", "alice@test.com")

        member = store.get_member("p1", "u1")

        assert member is not None
        assert member.role == Role.ADMIN

    def test_get_member_returns_none(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert store.get_member("p1", "u1") is None

    def test_remove_member(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.remove_member("p1", "u1")

        args = conn.execute.call_args
        assert "DELETE FROM project_members" in args[0][0]

    def test_ensure_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.ensure_tables()

        calls = conn.execute.call_args_list
        assert any("CREATE TABLE IF NOT EXISTS projects" in str(c) for c in calls)
        assert any("CREATE TABLE IF NOT EXISTS project_members" in str(c) for c in calls)

    def test_drop_tables(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.drop_tables()

        calls = conn.execute.call_args_list
        assert any("DROP TABLE IF EXISTS project_members" in str(c) for c in calls)
        assert any("DROP TABLE IF EXISTS projects" in str(c) for c in calls)

    def test_clean(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.clean()

        calls = conn.execute.call_args_list
        assert any("DELETE FROM project_members" in str(c) for c in calls)
        assert any("DELETE FROM projects" in str(c) for c in calls)
