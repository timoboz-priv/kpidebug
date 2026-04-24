from abc import ABC, abstractmethod

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


class ConnectorError(Exception):
    pass
