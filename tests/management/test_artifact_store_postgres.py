from unittest.mock import MagicMock

from kpidebug.management.artifact_store_postgres import PostgresArtifactStore
from kpidebug.management.types import ArtifactType


class TestPostgresArtifactStore:
    def _make_store(self) -> tuple[PostgresArtifactStore, MagicMock]:
        pool_manager = MagicMock()
        pool = MagicMock()
        pool_manager.pool.return_value = pool
        store = PostgresArtifactStore(pool_manager)
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
        assert any("CREATE TABLE IF NOT EXISTS project_artifacts" in c for c in calls)

    def test_create_url(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        result = store.create_url("p1", "https://docs.example.com")

        assert result.id != ""
        assert result.type == ArtifactType.URL
        assert result.value == "https://docs.example.com"
        assert result.created_at != ""
        conn.execute.assert_called_once()
        assert "INSERT INTO project_artifacts" in conn.execute.call_args[0][0]

    def test_create_file(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        result = store.create_file(
            project_id="p1",
            file_name="schema.sql",
            file_size=1234,
            file_mime_type="text/plain",
            file_content=b"CREATE TABLE test (id INT);",
        )

        assert result.id != ""
        assert result.type == ArtifactType.FILE
        assert result.file_name == "schema.sql"
        assert result.file_size == 1234
        assert result.file_mime_type == "text/plain"
        conn.execute.assert_called_once()
        params = conn.execute.call_args[0][1]
        assert b"CREATE TABLE test (id INT);" in params

    def test_list_artifacts(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = [
            ("a1", "p1", "url", "", "https://docs.example.com",
             "", 0, "", "2026-01-01T00:00:00Z"),
            ("a2", "p1", "file", "", "",
             "schema.sql", 1234, "text/plain", "2026-01-02T00:00:00Z"),
        ]

        results = store.list("p1")

        assert len(results) == 2
        assert results[0].type == ArtifactType.URL
        assert results[0].value == "https://docs.example.com"
        assert results[1].type == ArtifactType.FILE
        assert results[1].file_name == "schema.sql"
        assert results[1].file_size == 1234

    def test_list_empty(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchall.return_value = []

        assert store.list("p1") == []

    def test_delete_artifact(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)

        store.delete("p1", "a1")

        args = conn.execute.call_args
        assert "DELETE FROM project_artifacts" in args[0][0]
        assert args[0][1] == ("p1", "a1")

    def test_get_file_content(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = (b"file content here",)

        content = store.get_file_content("p1", "a1")

        assert content == b"file content here"

    def test_get_file_content_none(self):
        store, pool = self._make_store()
        conn = self._mock_connection(pool)
        conn.execute.return_value.fetchone.return_value = None

        assert store.get_file_content("p1", "nonexistent") is None
