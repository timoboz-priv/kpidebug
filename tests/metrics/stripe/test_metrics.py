from datetime import date, timedelta
from unittest.mock import MagicMock

from kpidebug.data.table_memory import InMemoryDataTable
from kpidebug.data.types import Aggregation, TableDescriptor, TableColumn, ColumnType
from kpidebug.metrics.context import MetricContext
from kpidebug.metrics.stripe.metrics import RevenueByUserTypeMetric, Retention30dMetric


def _charges_schema() -> TableDescriptor:
    return TableDescriptor(
        key="stripe:charges",
        name="Charges",
        columns=[
            TableColumn(key="id", name="ID", type=ColumnType.STRING),
            TableColumn(key="amount", name="Amount", type=ColumnType.NUMBER),
            TableColumn(key="customer", name="Customer", type=ColumnType.STRING),
            TableColumn(key="paid", name="Paid", type=ColumnType.STRING),
            TableColumn(key="created", name="Created", type=ColumnType.DATETIME),
        ],
    )


def _customers_schema() -> TableDescriptor:
    return TableDescriptor(
        key="stripe:customers",
        name="Customers",
        columns=[
            TableColumn(key="id", name="ID", type=ColumnType.STRING),
            TableColumn(key="created", name="Created", type=ColumnType.DATETIME),
        ],
    )


def _subscriptions_schema() -> TableDescriptor:
    return TableDescriptor(
        key="stripe:subscriptions",
        name="Subscriptions",
        columns=[
            TableColumn(key="id", name="ID", type=ColumnType.STRING),
            TableColumn(key="customer", name="Customer", type=ColumnType.STRING),
            TableColumn(key="status", name="Status", type=ColumnType.STRING),
            TableColumn(key="canceled_at", name="Canceled At", type=ColumnType.DATETIME),
            TableColumn(key="created", name="Created", type=ColumnType.DATETIME),
        ],
    )


def _make_ctx(tables: dict[str, InMemoryDataTable]) -> MetricContext:
    ctx = MagicMock(spec=MetricContext)
    ctx.table = lambda key: tables[key]
    return ctx


class TestRevenueByUserTypeMetric:
    def test_splits_revenue_by_new_and_returning(self):
        as_of = date(2025, 3, 15)
        window_start = as_of - timedelta(days=29)

        charges = InMemoryDataTable(_charges_schema(), [
            {"id": "ch_1", "amount": 100, "customer": "cust_new", "paid": "true",
             "created": f"{as_of.isoformat()}T10:00:00Z"},
            {"id": "ch_2", "amount": 200, "customer": "cust_old", "paid": "true",
             "created": f"{as_of.isoformat()}T12:00:00Z"},
        ])
        customers = InMemoryDataTable(_customers_schema(), [
            {"id": "cust_new", "created": f"{window_start.isoformat()}T00:00:00Z"},
            {"id": "cust_old", "created": "2024-01-01T00:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:charges": charges, "stripe:customers": customers})

        metric = RevenueByUserTypeMetric()
        results = metric.compute_single(ctx, dimensions=["user_type"], days=30, date=as_of)

        by_type = {r.groups["user_type"]: r.value for r in results}
        assert by_type["new"] == 100.0
        assert by_type["returning"] == 200.0

    def test_total_without_dimensions(self):
        as_of = date(2025, 3, 15)

        charges = InMemoryDataTable(_charges_schema(), [
            {"id": "ch_1", "amount": 100, "customer": "cust_1", "paid": "true",
             "created": f"{as_of.isoformat()}T10:00:00Z"},
            {"id": "ch_2", "amount": 50, "customer": "cust_2", "paid": "true",
             "created": f"{as_of.isoformat()}T12:00:00Z"},
        ])
        customers = InMemoryDataTable(_customers_schema(), [
            {"id": "cust_1", "created": "2024-01-01T00:00:00Z"},
            {"id": "cust_2", "created": "2024-06-01T00:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:charges": charges, "stripe:customers": customers})

        metric = RevenueByUserTypeMetric()
        results = metric.compute_single(ctx, days=30, date=as_of)

        assert len(results) == 1
        assert results[0].value == 150.0

    def test_excludes_unpaid_charges(self):
        as_of = date(2025, 3, 15)

        charges = InMemoryDataTable(_charges_schema(), [
            {"id": "ch_1", "amount": 100, "customer": "cust_1", "paid": "true",
             "created": f"{as_of.isoformat()}T10:00:00Z"},
            {"id": "ch_2", "amount": 999, "customer": "cust_1", "paid": "false",
             "created": f"{as_of.isoformat()}T12:00:00Z"},
        ])
        customers = InMemoryDataTable(_customers_schema(), [
            {"id": "cust_1", "created": "2024-01-01T00:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:charges": charges, "stripe:customers": customers})

        metric = RevenueByUserTypeMetric()
        results = metric.compute_single(ctx, dimensions=["user_type"], days=30, date=as_of)

        by_type = {r.groups["user_type"]: r.value for r in results}
        assert by_type.get("returning", 0) == 100.0
        assert "new" not in by_type

    def test_empty_charges(self):
        as_of = date(2025, 3, 15)

        charges = InMemoryDataTable(_charges_schema(), [])
        customers = InMemoryDataTable(_customers_schema(), [])
        ctx = _make_ctx({"stripe:charges": charges, "stripe:customers": customers})

        metric = RevenueByUserTypeMetric()
        results = metric.compute_single(ctx, dimensions=["user_type"], days=30, date=as_of)

        assert results == []


class TestRetention30dMetric:
    def test_full_retention(self):
        as_of = date(2025, 4, 15)
        cohort_date = as_of - timedelta(days=30)

        subs = InMemoryDataTable(_subscriptions_schema(), [
            {"id": "sub_1", "customer": "c1", "status": "active", "canceled_at": "",
             "created": f"{cohort_date.isoformat()}T10:00:00Z"},
            {"id": "sub_2", "customer": "c2", "status": "active", "canceled_at": "",
             "created": f"{cohort_date.isoformat()}T12:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:subscriptions": subs})

        metric = Retention30dMetric()
        results = metric.compute_single(ctx, days=1, date=as_of)

        assert len(results) == 1
        assert results[0].value == 100.0

    def test_partial_retention(self):
        as_of = date(2025, 4, 15)
        cohort_date = as_of - timedelta(days=30)
        canceled_before = (as_of - timedelta(days=5)).isoformat() + "T00:00:00Z"

        subs = InMemoryDataTable(_subscriptions_schema(), [
            {"id": "sub_1", "customer": "c1", "status": "active", "canceled_at": "",
             "created": f"{cohort_date.isoformat()}T10:00:00Z"},
            {"id": "sub_2", "customer": "c2", "status": "canceled",
             "canceled_at": canceled_before,
             "created": f"{cohort_date.isoformat()}T12:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:subscriptions": subs})

        metric = Retention30dMetric()
        results = metric.compute_single(ctx, days=1, date=as_of)

        assert len(results) == 1
        assert results[0].value == 50.0

    def test_canceled_after_as_of_counts_as_active(self):
        as_of = date(2025, 4, 15)
        cohort_date = as_of - timedelta(days=30)
        canceled_after = (as_of + timedelta(days=5)).isoformat() + "T00:00:00Z"

        subs = InMemoryDataTable(_subscriptions_schema(), [
            {"id": "sub_1", "customer": "c1", "status": "canceled",
             "canceled_at": canceled_after,
             "created": f"{cohort_date.isoformat()}T10:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:subscriptions": subs})

        metric = Retention30dMetric()
        results = metric.compute_single(ctx, days=1, date=as_of)

        assert results[0].value == 100.0

    def test_no_subscriptions_in_cohort(self):
        as_of = date(2025, 4, 15)

        subs = InMemoryDataTable(_subscriptions_schema(), [
            {"id": "sub_1", "customer": "c1", "status": "active", "canceled_at": "",
             "created": "2024-01-01T00:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:subscriptions": subs})

        metric = Retention30dMetric()
        results = metric.compute_single(ctx, days=1, date=as_of)

        assert results[0].value == 0.0

    def test_uses_cohort_window(self):
        as_of = date(2025, 4, 15)
        within_window = as_of - timedelta(days=29)
        outside_window = as_of - timedelta(days=40)

        subs = InMemoryDataTable(_subscriptions_schema(), [
            {"id": "sub_in", "customer": "c1", "status": "active", "canceled_at": "",
             "created": f"{within_window.isoformat()}T10:00:00Z"},
            {"id": "sub_out", "customer": "c2", "status": "canceled",
             "canceled_at": "2025-03-20T00:00:00Z",
             "created": f"{outside_window.isoformat()}T10:00:00Z"},
        ])
        ctx = _make_ctx({"stripe:subscriptions": subs})

        metric = Retention30dMetric()
        results = metric.compute_single(ctx, days=1, date=as_of)

        assert results[0].value == 100.0
