from datetime import date
from unittest.mock import MagicMock, patch, call

from kpidebug.analysis.types import AnalysisResult
from kpidebug.data.types import Aggregation
from kpidebug.metrics.types import (
    DashboardMetric,
    Metric,
    MetricSeriesResult,
    MetricSeriesPoint,
    MetricResult,
)
from kpidebug.processor import (
    ProcessMode,
    process_all,
    process_simulate,
)


def _make_dm(metric_id: str) -> DashboardMetric:
    return DashboardMetric(
        id=f"dm-{metric_id}",
        project_id="proj-1",
        metric_id=metric_id,
        aggregation=Aggregation.SUM,
    )


def _make_series(values: list[float]) -> MetricSeriesResult:
    points = [
        MetricSeriesPoint(results=[MetricResult(value=v)])
        for v in values
    ]
    return MetricSeriesResult(points=points)


def _make_stores(pinned: list[DashboardMetric] | None = None):
    data_source_store = MagicMock()
    data_source_store.list_sources = MagicMock(return_value=[])

    dashboard_store = MagicMock()
    dashboard_store.list_metrics = MagicMock(
        return_value=pinned or [],
    )

    metric_store = MagicMock()
    metric_store.get_definition = MagicMock(return_value=None)

    return data_source_store, dashboard_store, metric_store


class TestProcessMode:
    def test_enum_values(self):
        assert ProcessMode.FULL == "full"
        assert ProcessMode.METRICS == "metrics"
        assert ProcessMode.ANALYSIS == "analysis"


class TestProcessAll:
    @patch("kpidebug.processor._sync_data_sources")
    @patch("kpidebug.processor._compute_and_store_metrics")
    @patch("kpidebug.processor.MetricContext")
    def test_full_mode_runs_all_steps(
        self, mock_ctx_cls, mock_compute, mock_sync,
    ):
        mock_sync.return_value = 0.1
        mock_compute.return_value = 0.2
        mock_ctx_cls.for_project = MagicMock()
        ds, dash, ms = _make_stores()

        process_all("proj-1", ds, dash, ms, ProcessMode.FULL)

        mock_sync.assert_called_once()
        mock_compute.assert_called_once()

    @patch("kpidebug.processor._sync_data_sources")
    @patch("kpidebug.processor._compute_and_store_metrics")
    @patch("kpidebug.processor.MetricContext")
    def test_metrics_mode_skips_sync(
        self, mock_ctx_cls, mock_compute, mock_sync,
    ):
        mock_compute.return_value = 0.2
        mock_ctx_cls.for_project = MagicMock()
        ds, dash, ms = _make_stores()

        process_all("proj-1", ds, dash, ms, ProcessMode.METRICS)

        mock_sync.assert_not_called()
        mock_compute.assert_called_once()

    @patch("kpidebug.processor._sync_data_sources")
    @patch("kpidebug.processor._compute_and_store_metrics")
    @patch("kpidebug.processor.MetricContext")
    def test_analysis_mode_skips_sync_and_compute(
        self, mock_ctx_cls, mock_compute, mock_sync,
    ):
        mock_ctx_cls.for_project = MagicMock()
        ds, dash, ms = _make_stores()

        process_all(
            "proj-1", ds, dash, ms, ProcessMode.ANALYSIS,
        )

        mock_sync.assert_not_called()
        mock_compute.assert_not_called()

    @patch("kpidebug.processor._sync_data_sources")
    @patch("kpidebug.processor._compute_and_store_metrics")
    @patch("kpidebug.processor.MetricContext")
    def test_default_mode_is_full(
        self, mock_ctx_cls, mock_compute, mock_sync,
    ):
        mock_sync.return_value = 0.1
        mock_compute.return_value = 0.2
        mock_ctx_cls.for_project = MagicMock()
        ds, dash, ms = _make_stores()

        process_all("proj-1", ds, dash, ms)

        mock_sync.assert_called_once()
        mock_compute.assert_called_once()


class TestProcessSimulate:
    @patch("kpidebug.processor.registry")
    @patch("kpidebug.processor.MetricContext")
    def test_returns_analysis_result(
        self, mock_ctx_cls, mock_registry,
    ):
        mock_metric = MagicMock(spec=Metric)
        mock_metric.name = "Test Metric"
        mock_metric.compute_series = MagicMock(
            return_value=_make_series([100.0] * 14),
        )
        mock_registry.get = MagicMock(return_value=mock_metric)
        mock_ctx_cls.for_project = MagicMock()

        dm = _make_dm("builtin:test")
        ds, dash, ms = _make_stores([dm])

        result = process_simulate("proj-1", ds, dash, ms)

        assert isinstance(result, AnalysisResult)

    @patch("kpidebug.processor.registry")
    @patch("kpidebug.processor.MetricContext")
    def test_does_not_store_to_db(
        self, mock_ctx_cls, mock_registry,
    ):
        mock_metric = MagicMock(spec=Metric)
        mock_metric.name = "Test Metric"
        mock_metric.compute_series = MagicMock(
            return_value=_make_series([100.0] * 14),
        )
        mock_registry.get = MagicMock(return_value=mock_metric)
        mock_ctx_cls.for_project = MagicMock()

        dm = _make_dm("builtin:test")
        ds, dash, ms = _make_stores([dm])

        process_simulate("proj-1", ds, dash, ms)

        dash.store_snapshot.assert_not_called()

    @patch("kpidebug.processor.registry")
    @patch("kpidebug.processor.MetricContext")
    def test_passes_as_of_date_to_compute_series(
        self, mock_ctx_cls, mock_registry,
    ):
        target_date = date(2025, 6, 15)
        mock_metric = MagicMock(spec=Metric)
        mock_metric.name = "Test Metric"
        mock_metric.compute_series = MagicMock(
            return_value=_make_series([100.0] * 14),
        )
        mock_registry.get = MagicMock(return_value=mock_metric)
        mock_ctx_cls.for_project = MagicMock()

        dm = _make_dm("builtin:test")
        ds, dash, ms = _make_stores([dm])

        process_simulate(
            "proj-1", ds, dash, ms,
            as_of_date=target_date,
        )

        mock_metric.compute_series.assert_called_once()
        call_kwargs = mock_metric.compute_series.call_args
        assert call_kwargs.kwargs.get("date") == target_date

    @patch("kpidebug.processor.registry")
    @patch("kpidebug.processor.MetricContext")
    def test_does_not_sync_data_sources(
        self, mock_ctx_cls, mock_registry,
    ):
        mock_registry.get = MagicMock(return_value=None)
        mock_ctx_cls.for_project = MagicMock()

        ds, dash, ms = _make_stores()

        process_simulate("proj-1", ds, dash, ms)

        ds.list_sources.assert_not_called()

    @patch("kpidebug.processor.registry")
    @patch("kpidebug.processor.MetricContext")
    def test_empty_pinned_returns_empty_result(
        self, mock_ctx_cls, mock_registry,
    ):
        mock_ctx_cls.for_project = MagicMock()
        ds, dash, ms = _make_stores([])

        result = process_simulate("proj-1", ds, dash, ms)

        assert result.insights == []
