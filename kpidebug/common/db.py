import os

from psycopg_pool import ConnectionPool


class ConnectionPoolManager:
    _instance: "ConnectionPoolManager | None" = None
    _pool: ConnectionPool | None = None

    def __init__(self):
        database_url = os.getenv("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL environment variable is not set")
        self._pool = ConnectionPool(
            conninfo=database_url,
            min_size=2,
            max_size=10,
        )

    def pool(self) -> ConnectionPool:
        return self._pool

    def close(self) -> None:
        if self._pool is not None:
            self._pool.close()
            self._pool = None

    @staticmethod
    def get_instance() -> "ConnectionPoolManager":
        if ConnectionPoolManager._instance is None:
            ConnectionPoolManager._instance = ConnectionPoolManager()
        return ConnectionPoolManager._instance
