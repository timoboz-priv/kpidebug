from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.types import User
from kpidebug.management.user_store import AbstractUserStore


class PostgresUserStore(AbstractUserStore):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def get(self, user_id: str) -> User | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT id, name, email, avatar_url FROM users WHERE id = %s",
                (user_id,),
            ).fetchone()
        if row is None:
            return None
        return User(id=row[0], name=row[1], email=row[2], avatar_url=row[3])

    def create(self, user: User) -> User:
        with self.pool.connection() as conn:
            conn.execute(
                "INSERT INTO users (id, name, email, avatar_url) VALUES (%s, %s, %s, %s)",
                (user.id, user.name, user.email, user.avatar_url),
            )
        return user

    def update(self, user_id: str, updates: dict) -> User:
        if not updates:
            return self.get(user_id)
        set_clauses = ", ".join(f"{key} = %s" for key in updates)
        values = list(updates.values()) + [user_id]
        with self.pool.connection() as conn:
            conn.execute(
                f"UPDATE users SET {set_clauses} WHERE id = %s",
                values,
            )
        return self.get(user_id)

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL DEFAULT '',
                    email TEXT NOT NULL DEFAULT '',
                    avatar_url TEXT NOT NULL DEFAULT ''
                )
            """)
            conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_users_email
                ON users(email)
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS users CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM users")

    def get_by_email(self, email: str) -> User | None:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT id, name, email, avatar_url FROM users WHERE email = %s LIMIT 1",
                (email,),
            ).fetchone()
        if row is None:
            return None
        return User(id=row[0], name=row[1], email=row[2], avatar_url=row[3])

    def get_or_create(self, user_id: str, email: str | None, name: str | None, avatar_url: str | None) -> User:
        existing = self.get(user_id)
        if existing is not None:
            return existing
        user = User(
            id=user_id,
            name=name or "",
            email=email or "",
            avatar_url=avatar_url or "",
        )
        return self.create(user)
