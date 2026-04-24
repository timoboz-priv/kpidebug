from unittest.mock import MagicMock

from kpidebug.data.types import Row
from kpidebug.metrics.compute import compute_metric
from kpidebug.metrics.resolver import resolve_builtin, ResolvedMetric
from kpidebug.metrics.types import (
    Aggregation,
    MetricComputeInput,
    MetricDataType,
    MetricDefinition,
    MetricSource,
)


class TestComputeBuiltin:
    def test_computes_gross_revenue(self):
        resolved = resolve_builtin("builtin:stripe.gross_revenue")
        assert resolved is not None

        rows: list[Row] = [
            {"amount": 5000, "paid": True},
            {"amount": 3000, "paid": True},
            {"amount": 1000, "paid": False},
        ]
        compute_input = MetricComputeInput(rows=rows)
        results = compute_metric(resolved, compute_input)

        assert len(results) == 1
        assert results[0].value == 8000.0

    def test_computes_with_grouping(self):
        resolved = resolve_builtin("builtin:stripe.gross_revenue")
        assert resolved is not None

        rows: list[Row] = [
            {"amount": 5000, "paid": True, "currency": "usd"},
            {"amount": 3000, "paid": True, "currency": "eur"},
        ]
        compute_input = MetricComputeInput(
            rows=rows, group_by=["currency"],
        )
        results = compute_metric(resolved, compute_input)

        assert len(results) == 2
        by_group = {r.groups["currency"]: r.value for r in results}
        assert by_group["usd"] == 5000.0
        assert by_group["eur"] == 3000.0


class TestComputeExpression:
    def test_evaluates_expression(self):
        definition = MetricDefinition(
            id="m1",
            project_id="p1",
            name="Custom Sum",
            data_type=MetricDataType.NUMBER,
            source=MetricSource.EXPRESSION,
            computation="sum('amount')",
        )
        resolved = ResolvedMetric(
            id="m1",
            name="Custom Sum",
            data_type="number",
            source=MetricSource.EXPRESSION,
            definition=definition,
        )

        rows: list[Row] = [
            {"amount": 100},
            {"amount": 200},
        ]
        compute_input = MetricComputeInput(rows=rows)
        results = compute_metric(resolved, compute_input)

        assert len(results) == 1
        assert results[0].value == 300.0

    def test_applies_filters(self):
        from kpidebug.data.types import TableFilter
        definition = MetricDefinition(
            id="m1",
            project_id="p1",
            name="Filtered",
            data_type=MetricDataType.NUMBER,
            source=MetricSource.EXPRESSION,
            computation="sum('amount')",
        )
        resolved = ResolvedMetric(
            id="m1",
            name="Filtered",
            data_type="number",
            source=MetricSource.EXPRESSION,
            definition=definition,
        )

        rows: list[Row] = [
            {"amount": 100, "status": "active"},
            {"amount": 200, "status": "canceled"},
        ]
        compute_input = MetricComputeInput(
            rows=rows,
            filters=[TableFilter(column="status", operator="eq", value="active")],
        )
        results = compute_metric(resolved, compute_input)

        assert results[0].value == 100.0
