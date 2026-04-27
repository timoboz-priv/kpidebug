from kpidebug.data.table import TableRow
from kpidebug.data.table_memory import InMemoryDataTable
from kpidebug.data.types import (
    Aggregation,
    ColumnType,
    TableColumn,
    TableDescriptor,
    TableQuery,
    TableFilter,
)


def _sample_schema() -> TableDescriptor:
    return TableDescriptor(
        key="orders",
        name="Orders",
        columns=[
            TableColumn(key="id", name="ID", type=ColumnType.STRING, is_primary_key=True),
            TableColumn(key="amount", name="Amount", type=ColumnType.NUMBER),
            TableColumn(key="status", name="Status", type=ColumnType.STRING),
            TableColumn(key="currency", name="Currency", type=ColumnType.STRING),
        ],
    )


def _sample_rows() -> list[dict]:
    return [
        {"id": "1", "amount": 100, "status": "paid", "currency": "USD"},
        {"id": "2", "amount": 200, "status": "paid", "currency": "EUR"},
        {"id": "3", "amount": 50, "status": "refunded", "currency": "USD"},
        {"id": "4", "amount": 300, "status": "paid", "currency": "USD"},
        {"id": "5", "amount": 75, "status": "pending", "currency": "EUR"},
    ]


def _make_table() -> InMemoryDataTable:
    return InMemoryDataTable(_sample_schema(), _sample_rows())


class TestTableRow:
    def test_get_value(self):
        row = TableRow(_schema=_sample_schema(), _data={"id": "1", "amount": 100})
        assert row.get("id") == "1"
        assert row.get("amount") == 100

    def test_getitem(self):
        row = TableRow(_schema=_sample_schema(), _data={"id": "1", "amount": 100})
        assert row["id"] == "1"

    def test_get_missing_returns_none(self):
        row = TableRow(_schema=_sample_schema(), _data={"id": "1"})
        assert row.get("amount") is None

    def test_to_dict(self):
        data = {"id": "1", "amount": 100}
        row = TableRow(_schema=_sample_schema(), _data=data)
        assert row.to_dict() == data

    def test_schema_accessible(self):
        row = TableRow(_schema=_sample_schema(), _data={})
        assert row.schema.key == "orders"


class TestInMemoryDataTable:
    def test_count(self):
        table = _make_table()
        assert table.count() == 5

    def test_count_empty(self):
        table = InMemoryDataTable(_sample_schema(), [])
        assert table.count() == 0

    def test_get(self):
        table = _make_table()
        row = table.get(0)
        assert row["id"] == "1"
        assert row["amount"] == 100

    def test_rows(self):
        table = _make_table()
        rows = table.rows()
        assert len(rows) == 5
        assert all(isinstance(r, TableRow) for r in rows)

    def test_to_rows(self):
        table = _make_table()
        rows = table.to_rows()
        assert len(rows) == 5
        assert isinstance(rows[0], dict)

    def test_descriptor(self):
        table = _make_table()
        assert table.descriptor().key == "orders"
        assert len(table.descriptor().columns) == 4


class TestFilter:
    def test_filter_eq(self):
        result = _make_table().filter("status", "eq", "paid")
        assert result.count() == 3

    def test_filter_neq(self):
        result = _make_table().filter("status", "neq", "paid")
        assert result.count() == 2

    def test_filter_gt(self):
        result = _make_table().filter("amount", "gt", "100")
        assert result.count() == 2

    def test_filter_gte(self):
        result = _make_table().filter("amount", "gte", "100")
        assert result.count() == 3

    def test_filter_lt(self):
        result = _make_table().filter("amount", "lt", "100")
        assert result.count() == 2

    def test_filter_contains(self):
        result = _make_table().filter("status", "contains", "pai")
        assert result.count() == 3

    def test_filter_chain(self):
        result = _make_table().filter("status", "eq", "paid").filter("currency", "eq", "USD")
        assert result.count() == 2


class TestSelect:
    def test_select_columns(self):
        result = _make_table().select("id", "amount")
        assert len(result.descriptor().columns) == 2
        row = result.get(0)
        assert row.get("id") == "1"
        assert row.get("status") is None

    def test_select_preserves_count(self):
        result = _make_table().select("id")
        assert result.count() == 5


class TestLimit:
    def test_limit(self):
        result = _make_table().limit(3)
        assert result.count() == 3
        assert result.get(0)["id"] == "1"

    def test_limit_with_offset(self):
        result = _make_table().limit(2, offset=2)
        assert result.count() == 2
        assert result.get(0)["id"] == "3"


class TestSort:
    def test_sort_ascending(self):
        result = _make_table().sort("amount", ascending=True)
        amounts = [r["amount"] for r in result.to_rows()]
        assert amounts == [50, 75, 100, 200, 300]

    def test_sort_descending(self):
        result = _make_table().sort("amount", ascending=False)
        amounts = [r["amount"] for r in result.to_rows()]
        assert amounts == [300, 200, 100, 75, 50]


class TestGroupBy:
    def test_group_by_single_field(self):
        grouped = _make_table().group_by("status")
        assert len(grouped) == 3
        assert "paid" in grouped.keys()
        assert grouped["paid"].count() == 3

    def test_group_by_multiple_fields(self):
        grouped = _make_table().group_by("status", "currency")
        assert "paid|USD" in grouped.keys()
        assert grouped["paid|USD"].count() == 2

    def test_grouped_aggregate(self):
        grouped = _make_table().group_by("status")
        sums = grouped.aggregate("amount", Aggregation.SUM)
        assert sums["paid"] == 600
        assert sums["refunded"] == 50


class TestAggregate:
    def test_sum(self):
        assert _make_table().aggregate("amount", Aggregation.SUM) == 725

    def test_avg(self):
        assert _make_table().aggregate("amount", Aggregation.AVG) == 145.0

    def test_min(self):
        assert _make_table().aggregate("amount", Aggregation.MIN) == 50

    def test_max(self):
        assert _make_table().aggregate("amount", Aggregation.MAX) == 300

    def test_count(self):
        assert _make_table().aggregate("amount", Aggregation.COUNT) == 5

    def test_aggregate_empty_table(self):
        table = InMemoryDataTable(_sample_schema(), [])
        assert table.aggregate("amount", Aggregation.SUM) == 0.0


class TestJoin:
    def test_inner_join(self):
        left_schema = TableDescriptor(key="orders", name="Orders", columns=[
            TableColumn(key="id", name="ID"),
            TableColumn(key="customer_id", name="Customer ID"),
        ])
        right_schema = TableDescriptor(key="customers", name="Customers", columns=[
            TableColumn(key="customer_id", name="Customer ID"),
            TableColumn(key="name", name="Name"),
        ])
        left = InMemoryDataTable(left_schema, [
            {"id": "o1", "customer_id": "c1"},
            {"id": "o2", "customer_id": "c2"},
            {"id": "o3", "customer_id": "c1"},
        ])
        right = InMemoryDataTable(right_schema, [
            {"customer_id": "c1", "name": "Alice"},
            {"customer_id": "c2", "name": "Bob"},
        ])
        joined = left.join(right, on="customer_id")
        assert joined.count() == 3
        assert joined.get(0).to_dict()["name"] == "Alice"

    def test_join_no_match(self):
        schema = TableDescriptor(key="t", name="T", columns=[
            TableColumn(key="id", name="ID"),
        ])
        left = InMemoryDataTable(schema, [{"id": "1"}])
        right = InMemoryDataTable(schema, [{"id": "2"}])
        joined = left.join(right, on="id")
        assert joined.count() == 0


class TestUnion:
    def test_union(self):
        table1 = InMemoryDataTable(_sample_schema(), [_sample_rows()[0]])
        table2 = InMemoryDataTable(_sample_schema(), [_sample_rows()[1]])
        result = table1.union(table2)
        assert result.count() == 2


class TestQuery:
    def test_query_with_filters_and_sort(self):
        q = TableQuery(
            filters=[TableFilter(column="status", operator="eq", value="paid")],
            sort_by="amount",
            sort_order="desc",
            limit=2,
            offset=0,
        )
        result = _make_table().query(q)
        assert result.count() == 2
        assert result.get(0)["amount"] == 300


class TestChaining:
    def test_filter_sort_limit(self):
        result = (
            _make_table()
            .filter("status", "eq", "paid")
            .sort("amount", ascending=False)
            .limit(2)
        )
        assert result.count() == 2
        assert result.get(0)["amount"] == 300
        assert result.get(1)["amount"] == 200

    def test_filter_aggregate(self):
        value = _make_table().filter("status", "eq", "paid").aggregate("amount", Aggregation.SUM)
        assert value == 600

    def test_filter_group_aggregate(self):
        grouped = _make_table().filter("status", "eq", "paid").group_by("currency")
        sums = grouped.aggregate("amount", Aggregation.SUM)
        assert sums["USD"] == 400
        assert sums["EUR"] == 200


class TestAddColumn:
    def test_add_computed_column(self):
        result = _make_table().add_column(
            "double_amount", "Double Amount",
            lambda row: (row.get("amount") or 0) * 2,
        )
        assert result.count() == 5
        assert result.get(0)["double_amount"] == 200
        assert len(result.descriptor().columns) == 5


class TestFilterRows:
    def test_filter_rows_with_predicate(self):
        result = _make_table().filter_rows(lambda r: r.get("amount", 0) > 100)
        assert result.count() == 2
