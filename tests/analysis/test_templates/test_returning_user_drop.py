from unittest.mock import MagicMock

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.templates.returning_user_drop import ReturningUserDropTemplate
from kpidebug.data.types import Aggregation
from kpidebug.metrics.types import (
    DashboardMetric, Metric, MetricDimension, MetricResult,
    MetricSeriesPoint, MetricSeriesResult, MetricSnapshot,
)


def _make_snapshot(values: list[float]) -> MetricSnapshot:
    return MetricSnapshot(metric_id="test", project_id="proj-1", values=values)


def _make_dm(metric_id: str, snapshot: MetricSnapshot | None = None, aggregation: Aggregation = Aggregation.SUM) -> DashboardMetric:
    return DashboardMetric(
        id=f"dm-{metric_id}", project_id="proj-1", metric_id=metric_id,
        aggregation=aggregation, snapshot=snapshot,
    )


def _make_users_by_type_metric(
    new_recent: float,
    new_previous: float,
    ret_recent: float,
    ret_previous: float,
) -> Metric:
    metric = MagicMock(spec=Metric)
    metric.dimensions = [MetricDimension(key="new_vs_returning", name="User Type")]

    points = []
    for val_new, val_ret in [(new_previous, ret_previous)] * 7 + [(new_recent, ret_recent)] * 7:
        points.append(MetricSeriesPoint(results=[
            MetricResult(value=val_new, groups={"new_vs_returning": "new"}),
            MetricResult(value=val_ret, groups={"new_vs_returning": "returning"}),
        ]))

    metric.compute_series = MagicMock(return_value=MetricSeriesResult(points=points))
    return metric


def _make_revenue_by_user_type_metric(
    new_rev: float,
    returning_rev: float,
) -> Metric:
    metric = MagicMock(spec=Metric)
    metric.dimensions = [MetricDimension(key="user_type", name="User Type")]

    points = []
    for _ in range(14):
        points.append(MetricSeriesPoint(results=[
            MetricResult(value=new_rev, groups={"user_type": "new"}),
            MetricResult(value=returning_rev, groups={"user_type": "returning"}),
        ]))

    metric.compute_series = MagicMock(return_value=MetricSeriesResult(points=points))
    return metric


def _make_context(
    users_metric: Metric | None = None,
    engagement_snapshot: MetricSnapshot | None = None,
    revenue_snapshot: MetricSnapshot | None = None,
    rev_by_user_type_metric: Metric | None = None,
) -> AnalysisContext:
    dms: dict[str, DashboardMetric | None] = {
        "builtin:ga.engagement_rate": _make_dm("builtin:ga.engagement_rate", engagement_snapshot, Aggregation.AVG_DAILY) if engagement_snapshot else None,
        "builtin:stripe.gross_revenue": _make_dm("builtin:stripe.gross_revenue", revenue_snapshot) if revenue_snapshot else None,
    }

    metrics: dict[str, Metric | None] = {
        "builtin:ga.users_by_type": users_metric,
        "builtin:stripe.revenue_by_user_type": rev_by_user_type_metric,
    }

    ctx = MagicMock(spec=AnalysisContext)
    ctx.get_dashboard_metric = lambda mid: dms.get(mid)
    ctx.get_metric = lambda mid: metrics.get(mid)
    ctx._metric_context = MagicMock()
    return ctx


class TestReturningUserDropTemplate:
    def test_fires_on_returning_drop_with_stable_new(self):
        users_metric = _make_users_by_type_metric(
            new_recent=100, new_previous=100,
            ret_recent=60, ret_previous=100,
        )
        ctx = _make_context(users_metric=users_metric)

        template = ReturningUserDropTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        assert "returning" in result.headline.lower()
        assert len(result.signals) >= 2

    def test_does_not_fire_when_returning_stable(self):
        users_metric = _make_users_by_type_metric(
            new_recent=100, new_previous=100,
            ret_recent=100, ret_previous=100,
        )
        ctx = _make_context(users_metric=users_metric)

        template = ReturningUserDropTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_does_not_fire_when_new_users_also_changing(self):
        users_metric = _make_users_by_type_metric(
            new_recent=150, new_previous=100,
            ret_recent=60, ret_previous=100,
        )
        ctx = _make_context(users_metric=users_metric)

        template = ReturningUserDropTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_counterfactual_includes_revenue_recovery(self):
        users_metric = _make_users_by_type_metric(
            new_recent=100, new_previous=100,
            ret_recent=60, ret_previous=100,
        )
        rev_metric = _make_revenue_by_user_type_metric(
            new_rev=500, returning_rev=1000,
        )
        ctx = _make_context(
            users_metric=users_metric,
            rev_by_user_type_metric=rev_metric,
        )

        template = ReturningUserDropTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        assert result.counterfactual.value > 0
        assert result.counterfactual.revenue_impact.value > 0
        assert "Recovery" in result.counterfactual.revenue_impact.description

    def test_counterfactual_works_without_revenue_metric(self):
        users_metric = _make_users_by_type_metric(
            new_recent=100, new_previous=100,
            ret_recent=60, ret_previous=100,
        )
        ctx = _make_context(users_metric=users_metric)

        template = ReturningUserDropTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        assert result.counterfactual.value > 0
        assert result.counterfactual.revenue_impact.value == 0

    def test_returns_none_when_users_metric_missing(self):
        ctx = _make_context()

        template = ReturningUserDropTemplate()
        result = template.evaluate(ctx)

        assert result is None
