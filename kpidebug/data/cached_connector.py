from kpidebug.data.connector import DataSourceConnector
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import DataSource, TableDescriptor, TableQuery, TableResult


class CachedConnector(DataSourceConnector):

    def __init__(
        self, source: DataSource,
        live: DataSourceConnector,
        store: PostgresDataSourceStore,
    ):
        super().__init__(source)
        self.live: DataSourceConnector = live
        self.store: PostgresDataSourceStore = store

    def validate_credentials(self) -> bool:
        return self.live.validate_credentials()

    def get_tables(self) -> list[TableDescriptor]:
        return self.live.get_tables()

    def fetch_table_data(
        self,
        table_key: str,
        query: TableQuery | None = None,
    ) -> TableResult:
        q = query or TableQuery()

        cached = self.store.query_cached_rows(
            self.source.id, table_key, q,
        )
        if cached is not None:
            return cached

        return self.live.fetch_table_data(table_key, q)

    def fetch_all_rows(self, table_key: str) -> list[dict]:
        cached = self.store.get_cached_rows(
            self.source.id, table_key,
        )
        if cached is not None:
            return cached
        return self.live.fetch_all_rows(table_key)

    def sync_table(self, table_key: str) -> list[dict]:
        rows = self.live.fetch_all_rows(table_key)
        self.store.set_cached_rows(
            self.source.id, table_key, rows,
        )
        return rows

    def sync_all(self) -> dict[str, int]:
        results: dict[str, int] = {}
        for table in self.get_tables():
            rows = self.sync_table(table.key)
            results[table.key] = len(rows)
        return results
