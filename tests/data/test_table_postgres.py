import json
from unittest.mock import MagicMock

from kpidebug.data.table_postgres import PostgresDataTable
from kpidebug.data.types import (
    Aggregation,
    ColumnType,
    TableColumn,
    TableDescriptor,
)


def _sample_schema() -> TableDescriptor:
    return TableDescriptor(
        key="orders",
        name="Orders",
        columns=[
            TableColumn(key="id", name="ID", type=ColumnType.STRING),
            TableColumn(key="amount", name="Amount", type=ColumnType.NUMBER),
            TableColumn(key="status", name="Status", type=ColumnType.STRING),
        ],
    )


class TestPostgresDataTable:
    def _make_pool(self) -> MagicMock:
        pool = MagicMock()
        conn = MagicMock()
        pool.connection.return_value.__enter__ = MagicMock(return_value=conn)
        pool.connection.return_value.__exit__ = MagicMock(return_value=False)
        return pool

    def _make_table(self) -> tuple[PostgresDataTable, MagicMock]:
        pool = self._make_pool()
        table = PostgresDataTable(pool, "src1", "orders", _sample_schema())
        return table, pool

    def _get_conn(self, pool: MagicMock) -> MagicMock:
        return pool.connection.return_value.__enter__.return_value

    def test_count_executes_sql(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (5,)

        result = table.count()

        assert result == 5
        sql = conn.execute.call_args[0][0]
        assert "SELECT COUNT(*)" in sql
        assert "cached_table_data" in sql

    def test_filter_adds_where_clause(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (3,)

        filtered = table.filter("status", "eq", "paid")
        filtered.count()

        sql = conn.execute.call_args[0][0]
        assert "data->>'status' = %s" in sql

    def test_filter_numeric_comparison(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (2,)

        filtered = table.filter("amount", "gt", "100")
        filtered.count()

        sql = conn.execute.call_args[0][0]
        assert "(data->>'amount')::numeric > %s" in sql

    def test_filter_contains(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (1,)

        filtered = table.filter("status", "contains", "pai")
        filtered.count()

        sql = conn.execute.call_args[0][0]
        assert "ILIKE" in sql

    def test_sort_sets_order_by(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchall.return_value = []

        sorted_table = table.sort("amount", ascending=False)
        sorted_table.to_rows()

        sql = conn.execute.call_args[0][0]
        assert "ORDER BY data->>'amount' DESC" in sql

    def test_limit_adds_limit_offset(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchall.return_value = []

        limited = table.limit(10, offset=5)
        limited.to_rows()

        sql = conn.execute.call_args[0][0]
        assert "LIMIT %s OFFSET %s" in sql
        params = conn.execute.call_args[0][1]
        assert 10 in params
        assert 5 in params

    def test_select_reduces_schema(self):
        table, _ = self._make_table()
        selected = table.select("id", "amount")
        assert len(selected.descriptor().columns) == 2
        col_keys = [c.key for c in selected.descriptor().columns]
        assert "id" in col_keys
        assert "status" not in col_keys

    def test_to_rows_parses_jsonb(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchall.return_value = [
            ({"id": "1", "amount": 100, "status": "paid"},),
            ({"id": "2", "amount": 200, "status": "refunded"},),
        ]

        rows = table.to_rows()
        assert len(rows) == 2
        assert rows[0]["id"] == "1"

    def test_aggregate_sum(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (600.0,)

        result = table.aggregate("amount", Aggregation.SUM)

        assert result == 600.0
        sql = conn.execute.call_args[0][0]
        assert "SUM" in sql

    def test_aggregate_count(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (5,)

        result = table.aggregate("amount", Aggregation.COUNT)
        assert result == 5.0
        sql = conn.execute.call_args[0][0]
        assert "COUNT(*)" in sql

    def test_chaining_filter_then_aggregate(self):
        table, pool = self._make_table()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (400.0,)

        result = table.filter("status", "eq", "paid").aggregate("amount", Aggregation.SUM)

        assert result == 400.0
        sql = conn.execute.call_args[0][0]
        assert "data->>'status' = %s" in sql
        assert "SUM" in sql

    def test_from_rows_writes_to_cache(self):
        pool = self._make_pool()
        conn = self._get_conn(pool)

        rows = [
            {"id": "1", "amount": 100},
            {"id": "2", "amount": 200},
        ]
        result = PostgresDataTable.from_rows(pool, "src1", "orders", _sample_schema(), rows)

        assert isinstance(result, PostgresDataTable)
        calls = [str(c) for c in conn.execute.call_args_list]
        assert any("DELETE FROM cached_table_data" in c for c in calls)
        assert any("INSERT INTO cached_table_data" in c for c in calls)
        assert any("INSERT INTO cached_table_meta" in c for c in calls)

    def test_load_returns_none_when_not_cached(self):
        pool = self._make_pool()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = None

        result = PostgresDataTable.load(pool, "src1", "orders", _sample_schema())
        assert result is None

    def test_load_returns_table_when_cached(self):
        pool = self._make_pool()
        conn = self._get_conn(pool)
        conn.execute.return_value.fetchone.return_value = (1,)

        result = PostgresDataTable.load(pool, "src1", "orders", _sample_schema())
        assert isinstance(result, PostgresDataTable)
