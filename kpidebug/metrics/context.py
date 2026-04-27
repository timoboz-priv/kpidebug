from __future__ import annotations

from kpidebug.data.cached_connector import CachedConnector
from kpidebug.data.connector import DataSourceConnector
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.table import DataTable
from kpidebug.data.types import DataSource


class MetricContext:
    _connectors: list[DataSourceConnector]

    def __init__(self, connectors: list[DataSourceConnector]):
        self._connectors = connectors

    def table(self, table_key: str) -> DataTable:
        for connector in self._connectors:
            try:
                tables = connector.get_tables()
                if any(t.key == table_key for t in tables):
                    return connector.fetch_table(table_key)
            except Exception:
                continue
        raise ValueError(f"Table not found: {table_key}")

    @staticmethod
    def for_project(
        project_id: str,
        data_source_store: PostgresDataSourceStore,
    ) -> MetricContext:
        from kpidebug.api.routes_data_sources import make_connector
        sources = data_source_store.list_sources(project_id)
        connectors: list[DataSourceConnector] = []
        for source in sources:
            try:
                connectors.append(make_connector(source, data_source_store))
            except Exception:
                continue
        return MetricContext(connectors)
