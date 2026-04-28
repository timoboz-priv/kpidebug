from unittest.mock import MagicMock, patch

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.templates.acquisition_drop import AcquisitionDropTemplate
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
) -> AnalysisContext:
    ctx = MagicMock(spec=AnalysisContext)
    sessions_dm = _make_dm("builtin:ga.sessions", sessions_snapshot) if sessions_snapshot else None
    conversion_dm = _make_dm("builtin:ga.conversion_rate", conversion_snapshot, Aggregation.AVG_DAILY) if conversion_snapshot else None

    def get_dashboard_metric(metric_id: str) -> DashboardMetric | None:
        if metric_id == "builtin:ga.sessions":
            return sessions_dm
        if metric_id == "builtin:ga.conversion_rate":
            return conversion_dm
        return None

    ctx.get_dashboard_metric = get_dashboard_metric
    ctx.get_metric = MagicMock(return_value=None)
    return ctx


class TestAcquisitionDropTemplate:
    def test_fires_on_sessions_drop_with_stable_conversion(self):
        # 14 days: first 7 around 100, last 7 around 70 = ~30% drop
        sessions_values = [100.0] * 7 + [70.0] * 7
        # Conversion stable around 5%
        conversion_values = [5.0] * 14
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
        )

        template = AcquisitionDropTemplate()
        result = template.evaluate(ctx)

        assert result is not None
        assert "acquisition" in result.headline.lower() or "traffic" in result.headline.lower()
        assert len(result.signals) >= 2
        assert result.description
        assert result.upside_potential.value > 0
        assert result.upside_potential.metric_id == "builtin:ga.sessions"
        assert result.upside_potential.metric_name == "Sessions"
        assert result.upside_potential.description

    def test_does_not_fire_when_sessions_stable(self):
        sessions_values = [100.0] * 14
        conversion_values = [5.0] * 14
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
        )

        template = AcquisitionDropTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_does_not_fire_when_conversion_also_dropping(self):
        sessions_values = [100.0] * 7 + [70.0] * 7
        conversion_values = [5.0] * 7 + [3.0] * 7
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=_make_snapshot(conversion_values),
        )

        template = AcquisitionDropTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_returns_none_when_sessions_metric_missing(self):
        conversion_values = [5.0] * 14
        ctx = _make_context(
            sessions_snapshot=None,
            conversion_snapshot=_make_snapshot(conversion_values),
        )

        template = AcquisitionDropTemplate()
        result = template.evaluate(ctx)

        assert result is None

    def test_returns_none_when_conversion_metric_missing(self):
        sessions_values = [100.0] * 7 + [70.0] * 7
        ctx = _make_context(
            sessions_snapshot=_make_snapshot(sessions_values),
            conversion_snapshot=None,
        )

        template = AcquisitionDropTemplate()
        result = template.evaluate(ctx)

        assert result is None
