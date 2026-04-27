import uuid

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.types import Project, ProjectMember, Role
from kpidebug.management.project_store import AbstractProjectStore


class PostgresProjectStore(AbstractProjectStore):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS projects (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    description TEXT NOT NULL DEFAULT '',
                    summary TEXT DEFAULT NULL
                )
            """)
            conn.execute("""
                ALTER TABLE projects ADD COLUMN IF NOT EXISTS summary TEXT DEFAULT NULL
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS project_members (
                    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL DEFAULT 'read',
                    user_name TEXT NOT NULL DEFAULT '',
                    user_email TEXT NOT NULL DEFAULT '',
                    PRIMARY KEY (project_id, user_id)
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_project_members_user_id
                ON project_members(user_id)
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS project_members CASCADE")
            conn.execute("DROP TABLE IF EXISTS projects CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM project_members")
            conn.execute("DELETE FROM projects")

    def get(self, project_id: str) -> Project | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT id, name, description, summary FROM projects WHERE id = %s",
                (project_id,),
            ).fetchone()
        if row is None:
            return None
        return Project(id=row[0], name=row[1], description=row[2], summary=row[3])

    def create(self, name: str, description: str, creator_id: str, creator_name: str, creator_email: str) -> Project:
        project_id = str(uuid.uuid4())
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO projects (id, name, description) VALUES (%s, %s, %s)",
                (project_id, name, description),
            )
        self.add_member(project_id, creator_id, Role.ADMIN, creator_name, creator_email)
        return Project(id=project_id, name=name, description=description)

    def update(self, project_id: str, updates: dict) -> Project:
        if not updates:
            return self.get(project_id)
        set_clauses = ", ".join(f"{key} = %s" for key in updates)
        values = list(updates.values()) + [project_id]
        with self.pool.connection() as conn:
            conn.execute(
                f"UPDATE projects SET {set_clauses} WHERE id = %s",
                values,
            )
        return self.get(project_id)

    def delete(self, project_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM projects WHERE id = %s", (project_id,))

    def list_for_user(self, user_id: str) -> list[Project]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                """
                SELECT p.id, p.name, p.description, p.summary
                FROM projects p
                JOIN project_members pm ON pm.project_id = p.id
                WHERE pm.user_id = %s
                """,
                (user_id,),
            ).fetchall()
        return [Project(id=r[0], name=r[1], description=r[2], summary=r[3]) for r in rows]

    def get_members(self, project_id: str) -> list[ProjectMember]:
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT user_id, role, user_name, user_email FROM project_members WHERE project_id = %s",
                (project_id,),
            ).fetchall()
        return [
            ProjectMember(user_id=r[0], role=Role(r[1]), user_name=r[2], user_email=r[3])
            for r in rows
        ]

    def add_member(self, project_id: str, user_id: str, role: Role, user_name: str, user_email: str) -> ProjectMember:
        with self.pool.connection() as conn:
            conn.execute(
                """
                INSERT INTO project_members (project_id, user_id, role, user_name, user_email)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (project_id, user_id) DO UPDATE
                SET role = EXCLUDED.role, user_name = EXCLUDED.user_name, user_email = EXCLUDED.user_email
                """,
                (project_id, user_id, role.value, user_name, user_email),
            )
        return ProjectMember(user_id=user_id, role=role, user_name=user_name, user_email=user_email)

    def update_member_role(self, project_id: str, user_id: str, role: Role) -> ProjectMember:
        with self.pool.connection() as conn:
            conn.execute(
                "UPDATE project_members SET role = %s WHERE project_id = %s AND user_id = %s",
                (role.value, project_id, user_id),
            )
        return self.get_member(project_id, user_id)

    def remove_member(self, project_id: str, user_id: str) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM project_members WHERE project_id = %s AND user_id = %s",
                (project_id, user_id),
            )

    def get_member(self, project_id: str, user_id: str) -> ProjectMember | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT user_id, role, user_name, user_email FROM project_members WHERE project_id = %s AND user_id = %s",
                (project_id, user_id),
            ).fetchone()
        if row is None:
            return None
        return ProjectMember(user_id=row[0], role=Role(row[1]), user_name=row[2], user_email=row[3])
