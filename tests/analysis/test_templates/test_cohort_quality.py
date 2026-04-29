from unittest.mock import MagicMock

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.templates.cohort_quality import CohortQualityTemplate
from kpidebug.data.types import Aggregation
from kpidebug.metrics.types import DashboardMetric, MetricSnapshot


def _make_snapshot(values: list[float]) -> MetricSnapshot:
    return MetricSnapshot(metric_id="test", project_id="proj-1", values=values)


def _make_dm(metric_id: str, snapshot: MetricSnapshot | None = None, aggregation: Aggregation = Aggregation.AVG_DAILY) -> DashboardMetric:
    return DashboardMetric(
        id=f"dm-{metric_id}", project_id="proj-1", metric_id=metric_id,
        aggregation=aggregation, snapshot=snapshot,
    )


def _make_context(
    s2p_snapshot: MetricSnapshot | None = None,
    new_users_snapshot: MetricSnapshot | None = None,
    revenue_snapshot: MetricSnapshot | None = None,
    customer_snapshot: MetricSnapshot | None = None,
    retention_30d_snapshot: MetricSnapshot | None = None,
) -> AnalysisContext:
    dms: dict[str, DashboardMetric | None] = {
        "builtin:ga.signup_to_paid_rate": _make_dm("builtin:ga.signup_to_paid_rate", s2p_snapshot) if s2p_snapshot else None,
        "builtin:ga.new_users": _make_dm("builtin:ga.new_users", new_users_snapshot, Aggregation.SUM) if new_users_snapshot else None,
        "builtin:stripe.gross_revenue": _make_dm("builtin:stripe.gross_revenue", revenue_snapshot, Aggregation.SUM) if revenue_snapshot else None,
        "builtin:stripe.customer_count": _make_dm("builtin:stripe.customer_count", customer_snapshot, Aggregation.SUM) if customer_snapshot else None,
        "builtin:stripe.retention_30d": _make_dm("builtin:stripe.retention_30d", retention_30d_snapshot) if retention_30d_snapshot else None,
    }

    ctx = MagicMock(spec=AnalysisContext)
    ctx.get_dashboard_metric = lambda mid: dms.get(mid)
    ctx.get_metric = MagicMock(return_value=None)
    return ctx


class TestCohortQualityTemplate:
    def test_fires_on_low_signup_to_paid_rate(self):
        # Need 30 values (COMPARISON_WINDOW_DAYS). First 23 at 10%, last 7 at 5%.
        s2p_values = [10.0] * 23 + [5.0] * 7
        ctx = _make_context(s2p_snapshot=_make_snapshot(s2p_values))

        template = CohortQualityTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        assert "converting" in result.headline.lower() or "acquisition" in result.headline.lower()
        assert len(result.signals) >= 1
        assert result.signals[0].metric_id == "builtin:ga.signup_to_paid_rate"

    def test_does_not_fire_when_rate_stable(self):
        s2p_values = [10.0] * 30
        ctx = _make_context(s2p_snapshot=_make_snapshot(s2p_values))

        template = CohortQualityTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_adds_retention_signal_when_dropping(self):
        s2p_values = [10.0] * 23 + [5.0] * 7
        # Retention dropping: 85% -> 70%
        retention_values = [85.0] * 23 + [70.0] * 7
        ctx = _make_context(
            s2p_snapshot=_make_snapshot(s2p_values),
            retention_30d_snapshot=_make_snapshot(retention_values),
        )

        template = CohortQualityTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        metric_ids = [s.metric_id for s in result.signals]
        assert "builtin:stripe.retention_30d" in metric_ids

    def test_no_retention_signal_when_retention_stable(self):
        s2p_values = [10.0] * 23 + [5.0] * 7
        retention_values = [85.0] * 30
        ctx = _make_context(
            s2p_snapshot=_make_snapshot(s2p_values),
            retention_30d_snapshot=_make_snapshot(retention_values),
        )

        template = CohortQualityTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        metric_ids = [s.metric_id for s in result.signals]
        assert "builtin:stripe.retention_30d" not in metric_ids

    def test_fires_without_retention_metric(self):
        s2p_values = [10.0] * 23 + [5.0] * 7
        ctx = _make_context(s2p_snapshot=_make_snapshot(s2p_values))

        template = CohortQualityTemplate()
        result = template.evaluate(ctx)

        assert result is not None

    def test_returns_none_when_s2p_missing(self):
        ctx = _make_context()

        template = CohortQualityTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_returns_none_when_insufficient_data(self):
        s2p_values = [10.0] * 3
        ctx = _make_context(s2p_snapshot=_make_snapshot(s2p_values))

        template = CohortQualityTemplate()
        result = template.evaluate(ctx)

        assert result is None
