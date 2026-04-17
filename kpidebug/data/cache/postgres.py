import json

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.data.cache.base import TableCache


class PostgresTableCache(TableCache):
    def __init__(self, pool_manager: ConnectionPoolManager):
        self.pool = pool_manager.pool()

    def ensure_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS table_cache_rows (
                    source_id TEXT NOT NULL,
                    table_key TEXT NOT NULL,
                    row_index INTEGER NOT NULL,
                    data JSONB NOT NULL,
                    PRIMARY KEY (source_id, table_key, row_index)
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS table_cache_meta (
                    source_id TEXT NOT NULL,
                    table_key TEXT NOT NULL,
                    cached BOOLEAN NOT NULL DEFAULT TRUE,
                    PRIMARY KEY (source_id, table_key)
                )
            """)

    def drop_tables(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DROP TABLE IF EXISTS table_cache_rows CASCADE")
            conn.execute("DROP TABLE IF EXISTS table_cache_meta CASCADE")

    def clean(self) -> None:
        with self.pool.connection() as conn:
            conn.execute("DELETE FROM table_cache_rows")
            conn.execute("DELETE FROM table_cache_meta")

    def get_rows(
        self, source_id: str, table_key: str,
    ) -> list[dict] | None:
        if not self.is_cached(source_id, table_key):
            return None
        with self.pool.connection() as conn:
            rows = conn.execute(
                "SELECT data FROM table_cache_rows WHERE source_id = %s AND table_key = %s ORDER BY row_index",
                (source_id, table_key),
            ).fetchall()
        return [r[0] if isinstance(r[0], dict) else json.loads(r[0]) for r in rows]

    def set_rows(
        self, source_id: str, table_key: str,
        rows: list[dict],
    ) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM table_cache_rows WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            )
            for i, row in enumerate(rows):
                conn.execute(
                    "INSERT INTO table_cache_rows (source_id, table_key, row_index, data) VALUES (%s, %s, %s, %s)",
                    (source_id, table_key, i, json.dumps(row)),
                )
            conn.execute(
                """
                INSERT INTO table_cache_meta (source_id, table_key, cached)
                VALUES (%s, %s, TRUE)
                ON CONFLICT (source_id, table_key) DO UPDATE SET cached = TRUE
                """,
                (source_id, table_key),
            )

    def sync_rows(
        self, source_id: str, table_key: str,
        fresh_rows: list[dict],
        pk_columns: list[str],
    ) -> None:
        if not pk_columns or not self.is_cached(source_id, table_key):
            self.set_rows(source_id, table_key, fresh_rows)
            return

        existing = self.get_rows(source_id, table_key) or []

        cached_by_pk: dict[str, dict] = {}
        for row in existing:
            pk = _pk_value(row, pk_columns)
            cached_by_pk[pk] = row

        fresh_by_pk: dict[str, dict] = {}
        for row in fresh_rows:
            pk = _pk_value(row, pk_columns)
            fresh_by_pk[pk] = row

        merged: list[dict] = []
        for pk, row in fresh_by_pk.items():
            merged.append(row)

        self.set_rows(source_id, table_key, merged)

    def is_cached(
        self, source_id: str, table_key: str,
    ) -> bool:
        with self.pool.connection() as conn:
            row = conn.execute(
                "SELECT cached FROM table_cache_meta WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            ).fetchone()
        if row is None:
            return False
        return bool(row[0])

    def clear_table(
        self, source_id: str, table_key: str,
    ) -> None:
        with self.pool.connection() as conn:
            conn.execute(
                "DELETE FROM table_cache_rows WHERE source_id = %s AND table_key = %s",
                (source_id, table_key),
            )
            conn.execute(
                """
                INSERT INTO table_cache_meta (source_id, table_key, cached)
                VALUES (%s, %s, FALSE)
                ON CONFLICT (source_id, table_key) DO UPDATE SET cached = FALSE
                """,
                (source_id, table_key),
            )


def _pk_value(row: dict, pk_columns: list[str]) -> str:
    return "|".join(str(row.get(c, "")) for c in pk_columns)
