from unittest.mock import MagicMock

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.templates.conversion_breakdown import ConversionBreakdownTemplate
from kpidebug.data.types import Aggregation
from kpidebug.metrics.types import DashboardMetric, MetricSnapshot


def _make_snapshot(values: list[float]) -> MetricSnapshot:
    return MetricSnapshot(metric_id="test", project_id="proj-1", values=values)


def _make_dm(metric_id: str, snapshot: MetricSnapshot | None = None, aggregation: Aggregation = Aggregation.SUM) -> DashboardMetric:
    return DashboardMetric(
        id=f"dm-{metric_id}", project_id="proj-1", metric_id=metric_id,
        aggregation=aggregation, snapshot=snapshot,
    )


def _make_context(
    sessions_snapshot: MetricSnapshot | None = None,
    conversion_snapshot: MetricSnapshot | None = None,
    conversions_snapshot: MetricSnapshot | None = None,
    customers_snapshot: MetricSnapshot | None = None,
    revenue_snapshot: MetricSnapshot | None = None,
    signup_rate_snapshot: MetricSnapshot | None = None,
    signup_to_paid_snapshot: MetricSnapshot | None = None,
) -> AnalysisContext:
    dms: dict[str, DashboardMetric | None] = {
        "builtin:ga.sessions": _make_dm("builtin:ga.sessions", sessions_snapshot) if sessions_snapshot else None,
        "builtin:ga.conversion_rate": _make_dm("builtin:ga.conversion_rate", conversion_snapshot, Aggregation.AVG_DAILY) if conversion_snapshot else None,
        "builtin:ga.conversions": _make_dm("builtin:ga.conversions", conversions_snapshot) if conversions_snapshot else None,
        "builtin:stripe.customer_count": _make_dm("builtin:stripe.customer_count", customers_snapshot) if customers_snapshot else None,
        "builtin:stripe.gross_revenue": _make_dm("builtin:stripe.gross_revenue", revenue_snapshot) if revenue_snapshot else None,
        "builtin:ga.signup_rate": _make_dm("builtin:ga.signup_rate", signup_rate_snapshot, Aggregation.AVG_DAILY) if signup_rate_snapshot else None,
        "builtin:ga.signup_to_paid_rate": _make_dm("builtin:ga.signup_to_paid_rate", signup_to_paid_snapshot, Aggregation.AVG_DAILY) if signup_to_paid_snapshot else None,
    }

    ctx = MagicMock(spec=AnalysisContext)
    ctx.get_dashboard_metric = lambda mid: dms.get(mid)
    return ctx


class TestConversionBreakdownTemplate:
    def test_fires_on_conversion_drop_with_stable_traffic(self):
        sessions_values = [1000.0] * 14
        conversion_values = [5.0] * 7 + [3.5] * 7
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        assert "conversion" in result.headline.lower()
        assert len(result.signals) >= 2
        assert result.description
        assert result.counterfactual.value > 0
        assert result.counterfactual.metric_id == "builtin:ga.conversion_rate"
        assert result.counterfactual.metric_name == "Conversion Rate"
        assert result.counterfactual.description

    def test_does_not_fire_when_traffic_also_dropping(self):
        sessions_values = [1000.0] * 7 + [700.0] * 7
        conversion_values = [5.0] * 7 + [3.5] * 7
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_does_not_fire_when_conversion_stable(self):
        sessions_values = [1000.0] * 14
        conversion_values = [5.0] * 14
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_adds_corroborating_signals_when_available(self):
        sessions_values = [1000.0] * 14
        conversion_values = [5.0] * 7 + [3.5] * 7
        conversions_values = [50.0] * 7 + [35.0] * 7
        revenue_values = [5000.0] * 7 + [3500.0] * 7
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
            conversions_snapshot=_make_snapshot(conversions_values),
            revenue_snapshot=_make_snapshot(revenue_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        metric_ids = [s.metric_id for s in result.signals]
        assert "builtin:ga.conversions" in metric_ids
        assert "builtin:stripe.gross_revenue" in metric_ids

    def test_funnel_analysis_conversions_drop_customers_stable(self):
        sessions_values = [1000.0] * 14
        conversion_values = [5.0] * 7 + [3.5] * 7
        conversions_values = [50.0] * 7 + [35.0] * 7
        customers_values = [20.0] * 14
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
            conversions_snapshot=_make_snapshot(conversions_values),
            customers_snapshot=_make_snapshot(customers_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        action_texts = [a.description for a in result.actions]
        assert any("lead quality" in a.lower() or "event tracking" in a.lower() for a in action_texts)

    def test_funnel_analysis_customers_drop_conversions_stable(self):
        sessions_values = [1000.0] * 14
        conversion_values = [5.0] * 7 + [3.5] * 7
        conversions_values = [50.0] * 14
        customers_values = [20.0] * 7 + [12.0] * 7
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
            conversions_snapshot=_make_snapshot(conversions_values),
            customers_snapshot=_make_snapshot(customers_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        action_texts = [a.description for a in result.actions]
        assert any("signup-to-paid" in a.lower() or "payment" in a.lower() for a in action_texts)

    def test_funnel_steps_signup_rate_dropping(self):
        sessions_values = [1000.0] * 14
        conversion_values = [5.0] * 7 + [3.5] * 7
        signup_values = [10.0] * 7 + [6.0] * 7
        s2p_values = [50.0] * 14
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
            signup_rate_snapshot=_make_snapshot(signup_values),
            signup_to_paid_snapshot=_make_snapshot(s2p_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        metric_ids = [s.metric_id for s in result.signals]
        assert "builtin:ga.signup_rate" in metric_ids
        action_texts = [a.description.lower() for a in result.actions]
        assert any("signup" in a and "weakest" in a for a in action_texts)

    def test_funnel_steps_signup_to_paid_dropping(self):
        sessions_values = [1000.0] * 14
        conversion_values = [5.0] * 7 + [3.5] * 7
        signup_values = [10.0] * 14
        s2p_values = [50.0] * 7 + [30.0] * 7
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
            signup_rate_snapshot=_make_snapshot(signup_values),
            signup_to_paid_snapshot=_make_snapshot(s2p_values),
        )

        template = ConversionBreakdownTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        metric_ids = [s.metric_id for s in result.signals]
        assert "builtin:ga.signup_to_paid_rate" in metric_ids
        action_texts = [a.description.lower() for a in result.actions]
        assert any("signup-to-paid" in a and "weakest" in a for a in action_texts)

    def test_returns_none_when_metrics_missing(self):
        template = ConversionBreakdownTemplate()

        assert template.evaluate(_make_context(sessions_snapshot=_make_snapshot([100.0] * 14))) is None
        assert template.evaluate(_make_context(conversion_snapshot=_make_snapshot([5.0] * 14))) is None
        assert template.evaluate(_make_context()) is None
