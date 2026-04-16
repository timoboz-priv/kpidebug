from abc import ABC, abstractmethod


class TableCache(ABC):
    @abstractmethod
    def get_rows(
        self, source_id: str, table_key: str,
    ) -> list[dict] | None:
        """Return all cached rows, or None if not cached."""
        ...

    @abstractmethod
    def set_rows(
        self, source_id: str, table_key: str,
        rows: list[dict],
    ) -> None:
        """Store all rows for a table, replacing any
        existing cached data."""
        ...

    @abstractmethod
    def sync_rows(
        self, source_id: str, table_key: str,
        fresh_rows: list[dict],
        pk_columns: list[str],
    ) -> None:
        """Sync cache with fresh data by comparing
        primary keys. Upserts new/changed rows, removes
        rows that no longer exist in the source."""
        ...

    @abstractmethod
    def is_cached(
        self, source_id: str, table_key: str,
    ) -> bool:
        """Check if this table has cached data."""
        ...

    @abstractmethod
    def clear_table(
        self, source_id: str, table_key: str,
    ) -> None:
        """Clear cache for a specific table."""
        ...
