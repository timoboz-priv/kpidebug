from abc import ABC, abstractmethod

from kpidebug.data.table import DataTable
from kpidebug.data.types import DataSource, Row, TableDescriptor, TableQuery, TableResult


class DataSourceConnector(ABC):

    def __init__(self, source: DataSource):
        self.source: DataSource = source

    @abstractmethod
    def validate_credentials(self) -> bool:
        ...

    @abstractmethod
    def get_tables(self) -> list[TableDescriptor]:
        ...

    @abstractmethod
    def fetch_table_data(
        self,
        table_key: str,
        query: TableQuery | None = None,
    ) -> TableResult:
        ...

    @abstractmethod
    def fetch_all_rows(
        self,
        table_key: str,
    ) -> list[Row]:
        ...

    def fetch_table(self, table_key: str) -> DataTable:
        from kpidebug.data.table_memory import InMemoryDataTable
        tables = self.get_tables()
        schema = next((t for t in tables if t.key == table_key), None)
        if schema is None:
            schema = TableDescriptor(key=table_key, name=table_key)
        rows = self.fetch_all_rows(table_key)
        return InMemoryDataTable(schema, rows)


class ConnectorError(Exception):
    pass
