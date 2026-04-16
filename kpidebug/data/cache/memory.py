from kpidebug.data.cache.base import TableCache


class InMemoryTableCache(TableCache):
    _tables: dict[str, list[dict]]

    def __init__(self):
        self._tables = {}

    def _key(self, source_id: str, table_key: str) -> str:
        return f"{source_id}:{table_key}"

    def get_rows(
        self, source_id: str, table_key: str,
    ) -> list[dict] | None:
        return self._tables.get(self._key(source_id, table_key))

    def set_rows(
        self, source_id: str, table_key: str,
        rows: list[dict],
    ) -> None:
        self._tables[self._key(source_id, table_key)] = list(rows)

    def sync_rows(
        self, source_id: str, table_key: str,
        fresh_rows: list[dict],
        pk_columns: list[str],
    ) -> None:
        # For in-memory, a full replacement is sufficient
        self.set_rows(source_id, table_key, fresh_rows)

    def is_cached(
        self, source_id: str, table_key: str,
    ) -> bool:
        return self._key(source_id, table_key) in self._tables

    def clear_table(
        self, source_id: str, table_key: str,
    ) -> None:
        self._tables.pop(
            self._key(source_id, table_key), None
        )
