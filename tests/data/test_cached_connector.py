from unittest.mock import MagicMock

from kpidebug.data.cached_connector import CachedConnector
from kpidebug.data.types import (
    DataSource,
    DataSourceType,
    TableDescriptor,
    TableFilter,
    TableQuery,
    TableResult,
)


def _make_cached_connector() -> tuple[CachedConnector, MagicMock, MagicMock]:
    source = DataSource(
        id="s1", project_id="p1",
        type=DataSourceType.STRIPE,
        credentials={"api_key": "sk_test"},
    )
    live = MagicMock()
    store = MagicMock()
    connector = CachedConnector(source, live, store)
    return connector, live, store


class TestCachedConnectorFetchTableData:
    def test_returns_cached_rows_when_available(self):
        connector, live, store = _make_cached_connector()
        store.query_cached_rows.return_value = TableResult(
            rows=[{"id": "1"}], total_count=1,
        )

        result = connector.fetch_table_data("charges")

        store.query_cached_rows.assert_called_once()
        live.fetch_table_data.assert_not_called()
        assert result.rows == [{"id": "1"}]
        assert result.total_count == 1

    def test_falls_back_to_live_when_not_cached(self):
        connector, live, store = _make_cached_connector()
        store.query_cached_rows.return_value = None
        live.fetch_table_data.return_value = TableResult(
            rows=[{"id": "2"}], total_count=1,
        )

        result = connector.fetch_table_data("charges")

        live.fetch_table_data.assert_called_once()
        assert result.rows == [{"id": "2"}]

    def test_passes_query_to_cache(self):
        connector, live, store = _make_cached_connector()
        store.query_cached_rows.return_value = TableResult(
            rows=[{"id": "1", "status": "succeeded"}], total_count=1,
        )
        query = TableQuery(
            filters=[TableFilter(column="status", operator="eq", value="succeeded")],
            sort_by="id",
            sort_order="asc",
            limit=10,
            offset=0,
        )

        result = connector.fetch_table_data("charges", query)

        call_args = store.query_cached_rows.call_args
        assert call_args[0][0] == "s1"
        assert call_args[0][1] == "charges"
        passed_query = call_args[0][2]
        assert len(passed_query.filters) == 1
        assert result.total_count == 1


class TestCachedConnectorSync:
    def test_sync_table_fetches_all_and_stores(self):
        connector, live, store = _make_cached_connector()
        live.fetch_all_rows.return_value = [
            {"id": "1"}, {"id": "2"},
        ]

        rows = connector.sync_table("charges")

        live.fetch_all_rows.assert_called_once_with("charges")
        store.set_cached_rows.assert_called_once_with(
            "s1", "charges", [{"id": "1"}, {"id": "2"}],
        )
        assert len(rows) == 2

    def test_sync_all_syncs_every_table(self):
        connector, live, store = _make_cached_connector()
        live.get_tables.return_value = [
            TableDescriptor(key="charges"),
            TableDescriptor(key="customers"),
        ]
        live.fetch_all_rows.return_value = [{"id": "1"}]

        results = connector.sync_all()

        assert results == {"charges": 1, "customers": 1}
        assert live.fetch_all_rows.call_count == 2
        assert store.set_cached_rows.call_count == 2


class TestCachedConnectorDelegation:
    def test_validate_credentials_delegates(self):
        connector, live, store = _make_cached_connector()
        live.validate_credentials.return_value = True

        assert connector.validate_credentials() is True
        live.validate_credentials.assert_called_once()

    def test_get_tables_delegates(self):
        connector, live, store = _make_cached_connector()
        live.get_tables.return_value = [TableDescriptor(key="t1")]

        tables = connector.get_tables()

        assert len(tables) == 1
        live.get_tables.assert_called_once()
