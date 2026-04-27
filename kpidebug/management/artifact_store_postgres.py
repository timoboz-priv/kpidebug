import uuid
from datetime import datetime, timezone

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.artifact_store import AbstractArtifactStore
from kpidebug.management.types import ArtifactType, ProjectArtifact


class PostgresArtifactStore(AbstractArtifactStore):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS project_artifacts (
                    id TEXT NOT NULL,
                    project_id TEXT NOT NULL,
                    type TEXT NOT NULL DEFAULT 'url',
                    name TEXT NOT NULL DEFAULT '',
                    value TEXT NOT NULL DEFAULT '',
                    file_name TEXT NOT NULL DEFAULT '',
                    file_size INTEGER NOT NULL DEFAULT 0,
                    file_mime_type TEXT NOT NULL DEFAULT '',
                    file_content BYTEA,
                    created_at TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (project_id, id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_artifacts_project_id
                ON project_artifacts(project_id)
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS project_artifacts CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM project_artifacts")

    def create_url(self, project_id: str, url: str) -> ProjectArtifact:
        artifact_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with self.pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO project_artifacts
                    (id, project_id, type, value, created_at)
                VALUES (%s, %s, %s, %s, %s)
                """,
                (artifact_id, project_id, ArtifactType.URL.value, url, now),
            )

        return ProjectArtifact(
            id=artifact_id,
            project_id=project_id,
            type=ArtifactType.URL,
            value=url,
            created_at=now,
        )

    def create_file(
        self,
        project_id: str,
        file_name: str,
        file_size: int,
        file_mime_type: str,
        file_content: bytes,
    ) -> ProjectArtifact:
        artifact_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

        with self.pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO project_artifacts
                    (id, project_id, type, file_name, file_size, file_mime_type,
                     file_content, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    artifact_id, project_id, ArtifactType.FILE.value,
                    file_name, file_size, file_mime_type,
                    file_content, now,
                ),
            )

        return ProjectArtifact(
            id=artifact_id,
            project_id=project_id,
            type=ArtifactType.FILE,
            file_name=file_name,
            file_size=file_size,
            file_mime_type=file_mime_type,
            created_at=now,
        )

    def list(self, project_id: str) -> list[ProjectArtifact]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT id, project_id, type, name, value,
                       file_name, file_size, file_mime_type, created_at
                FROM project_artifacts
                WHERE project_id = %s
                ORDER BY created_at
                """,
                (project_id,),
            ).fetchall()
        return [self._row_to_artifact(r) for r in rows]

    def get_file_content(self, project_id: str, artifact_id: str) -> bytes | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT file_content FROM project_artifacts WHERE project_id = %s AND id = %s",
                (project_id, artifact_id),
            ).fetchone()
        if row is None or row[0] is None:
            return None
        return bytes(row[0])

    def delete(self, project_id: str, artifact_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM project_artifacts WHERE project_id = %s AND id = %s",
                (project_id, artifact_id),
            )

    def _row_to_artifact(self, row: tuple) -> ProjectArtifact:
        return ProjectArtifact(
            id=row[0],
            project_id=row[1],
            type=ArtifactType(row[2]),
            name=row[3],
            value=row[4],
            file_name=row[5],
            file_size=int(row[6]),
            file_mime_type=row[7],
            created_at=row[8],
        )
