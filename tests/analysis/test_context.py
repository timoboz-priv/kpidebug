from unittest.mock import MagicMock, patch

from kpidebug.analysis.context import AnalysisContext
from kpidebug.data.types import TableDescriptor
from kpidebug.metrics.types import DashboardMetric, Metric, MetricDefinition, MetricDataType


def _make_dashboard_metric(metric_id: str) -> DashboardMetric:
    return DashboardMetric(id=f"dm-{metric_id}", project_id="proj-1", metric_id=metric_id)


def _make_context(
    dashboard_metrics: list[DashboardMetric] | None = None,
) -> AnalysisContext:
    metric_context = MagicMock()
    metric_context._connectors = []
    metric_store = MagicMock()
    metric_store.get_definition = MagicMock(return_value=None)
    data_source_store = MagicMock()

    return AnalysisContext(
        project_id="proj-1",
        dashboard_metrics=dashboard_metrics or [],
        metric_context=metric_context,
        metric_store=metric_store,
        data_source_store=data_source_store,
    )


class TestAnalysisContext:
    def test_dashboard_metrics_returns_provided_list(self):
        dm1 = _make_dashboard_metric("m1")
        dm2 = _make_dashboard_metric("m2")
        ctx = _make_context([dm1, dm2])

        assert ctx.dashboard_metrics == [dm1, dm2]

    @patch("kpidebug.analysis.context.registry")
    def test_get_metric_resolves_and_caches(self, mock_registry: MagicMock):
        mock_metric = MagicMock(spec=Metric)
        mock_registry.get = MagicMock(return_value=mock_metric)

        ctx = _make_context()
        first = ctx.get_metric("builtin:test")
        second = ctx.get_metric("builtin:test")

        assert first is mock_metric
        assert second is mock_metric
        mock_registry.get.assert_called_once_with("builtin:test")

    @patch("kpidebug.analysis.context.registry")
    def test_get_metric_returns_none_for_unknown(self, mock_registry: MagicMock):
        mock_registry.get = MagicMock(return_value=None)
        ctx = _make_context()

        result = ctx.get_metric("nonexistent")

        assert result is None

    @patch("kpidebug.analysis.context.registry")
    def test_get_metric_falls_back_to_definition(self, mock_registry: MagicMock):
        mock_registry.get = MagicMock(return_value=None)
        definition = MetricDefinition(
            id="custom-1", project_id="proj-1", name="Custom",
            description="A custom metric", data_type=MetricDataType.NUMBER,
            computation="sum(amount) from charges",
        )

        ctx = _make_context()
        ctx._metric_store.get_definition = MagicMock(return_value=definition)

        result = ctx.get_metric("custom-1")

        assert result is not None
        assert result.id == "custom-1"
        assert result.name == "Custom"

    @patch("kpidebug.analysis.context.registry")
    def test_list_metrics_resolves_all(self, mock_registry: MagicMock):
        metric_a = MagicMock(spec=Metric)
        metric_b = MagicMock(spec=Metric)
        mock_registry.get = MagicMock(side_effect=lambda mid: {"m1": metric_a, "m2": metric_b}.get(mid))

        dm1 = _make_dashboard_metric("m1")
        dm2 = _make_dashboard_metric("m2")
        ctx = _make_context([dm1, dm2])

        result = ctx.list_metrics()

        assert result == [metric_a, metric_b]

    @patch("kpidebug.analysis.context.registry")
    def test_list_metrics_skips_unresolvable(self, mock_registry: MagicMock):
        metric_a = MagicMock(spec=Metric)
        mock_registry.get = MagicMock(side_effect=lambda mid: {"m1": metric_a}.get(mid))

        dm1 = _make_dashboard_metric("m1")
        dm2 = _make_dashboard_metric("unknown")
        ctx = _make_context([dm1, dm2])

        result = ctx.list_metrics()

        assert result == [metric_a]

    def test_get_table_delegates_and_caches(self):
        mock_table = MagicMock()
        ctx = _make_context()
        ctx._metric_context.table = MagicMock(return_value=mock_table)

        first = ctx.get_table("stripe:charges")
        second = ctx.get_table("stripe:charges")

        assert first is mock_table
        assert second is mock_table
        ctx._metric_context.table.assert_called_once_with("stripe:charges")

    def test_list_tables_collects_from_connectors(self):
        connector_a = MagicMock()
        connector_a.source = MagicMock()
        connector_a.source.id = "src-1"
        connector_a.get_tables = MagicMock(return_value=[
            TableDescriptor(key="stripe:charges", name="Charges"),
        ])

        connector_b = MagicMock()
        connector_b.source = MagicMock()
        connector_b.source.id = "src-2"
        connector_b.get_tables = MagicMock(return_value=[
            TableDescriptor(key="ga:sessions", name="Sessions"),
            TableDescriptor(key="ga:pages", name="Pages"),
        ])

        ctx = _make_context()
        ctx._metric_context._connectors = [connector_a, connector_b]

        result = ctx.list_tables()

        assert len(result) == 3
        assert result[0].key == "stripe:charges"
        assert result[1].key == "ga:sessions"
        assert result[2].key == "ga:pages"

    def test_list_tables_caches_result(self):
        connector = MagicMock()
        connector.source = MagicMock()
        connector.source.id = "src-1"
        connector.get_tables = MagicMock(return_value=[
            TableDescriptor(key="t1", name="T1"),
        ])

        ctx = _make_context()
        ctx._metric_context._connectors = [connector]

        first = ctx.list_tables()
        second = ctx.list_tables()

        assert first is second
        connector.get_tables.assert_called_once()

    def test_list_tables_handles_connector_error(self):
        good_connector = MagicMock()
        good_connector.source = MagicMock()
        good_connector.source.id = "src-1"
        good_connector.get_tables = MagicMock(return_value=[
            TableDescriptor(key="t1", name="T1"),
        ])

        bad_connector = MagicMock()
        bad_connector.source = MagicMock()
        bad_connector.source.id = "src-2"
        bad_connector.get_tables = MagicMock(side_effect=Exception("connection failed"))

        ctx = _make_context()
        ctx._metric_context._connectors = [good_connector, bad_connector]

        result = ctx.list_tables()

        assert len(result) == 1
        assert result[0].key == "t1"
