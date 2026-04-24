import logging
from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json

from kpidebug.data.connector import DataSourceConnector
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import DataSource, TableDescriptor, TableQuery, TableResult

logger = logging.getLogger(__name__)


@dataclass_json
@dataclass
class TableSyncError:
    table: str = ""
    error: str = ""


@dataclass_json
@dataclass
class SyncAllResult:
    tables: dict[str, int] = dataclass_field(default_factory=dict)
    errors: list[TableSyncError] = dataclass_field(default_factory=list)


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

    def sync_all(self) -> SyncAllResult:
        result = SyncAllResult()
        for table in self.get_tables():
            try:
                rows = self.sync_table(table.key)
                result.tables[table.key] = len(rows)
            except Exception as e:
                logger.warning("Failed to sync table %s: %s", table.key, e)
                result.errors.append(TableSyncError(
                    table=table.key, error=str(e),
                ))
        return result
