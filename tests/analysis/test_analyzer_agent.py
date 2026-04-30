from unittest.mock import MagicMock, patch

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.types import AnalysisResult, Insight, Priority
from kpidebug.data.types import (
    Aggregation,
    ColumnType,
    FilterOperator,
    TableColumn,
    TableDescriptor,
)
from kpidebug.metrics.types import (
    DashboardMetric,
    Metric,
    MetricDataType,
    MetricDimension,
    MetricResult,
    MetricSeriesPoint,
    MetricSeriesResult,
    MetricSnapshot,
)

import kpidebug.analysis.analyzer_agent as agentic


def _make_snapshot(
    values: list[float], metric_id: str = "test",
) -> MetricSnapshot:
    return MetricSnapshot(
        metric_id=metric_id, project_id="proj-1", values=values,
    )


def _make_dm(
    metric_id: str,
    snapshot: MetricSnapshot | None = None,
    aggregation: Aggregation = Aggregation.SUM,
) -> DashboardMetric:
    return DashboardMetric(
        id=f"dm-{metric_id}",
        project_id="proj-1",
        metric_id=metric_id,
        aggregation=aggregation,
        snapshot=snapshot,
    )


def _make_metric(
    metric_id: str = "test",
    name: str = "Test Metric",
    dimensions: list[MetricDimension] | None = None,
    table_keys: list[str] | None = None,
) -> Metric:
    m = MagicMock(spec=Metric)
    m.id = metric_id
    m.name = name
    m.description = "A test metric"
    m.data_type = MetricDataType.NUMBER
    m.dimensions = dimensions or []
    m.table_keys = table_keys or []
    return m


def _setup_context(
    dashboard_metrics: list[DashboardMetric] | None = None,
    metrics: dict[str, Metric] | None = None,
    tables: list[TableDescriptor] | None = None,
) -> AnalysisContext:
    ctx = MagicMock(spec=AnalysisContext)
    dms = dashboard_metrics or []
    ctx.dashboard_metrics = dms
    ctx.as_of_date = None

    dm_map = {dm.metric_id: dm for dm in dms}
    ctx.get_dashboard_metric = (
        lambda mid: dm_map.get(mid)
    )

    metric_map = metrics or {}
    ctx.get_metric = lambda mid: metric_map.get(mid)
    ctx.list_metrics = lambda: list(metric_map.values())

    ctx.list_tables = lambda: tables or []
    return ctx


def _set_context(ctx: AnalysisContext) -> None:
    agentic._context = ctx


def _clear_context() -> None:
    agentic._context = None


# -------------------------------------------------------------------
# list_dashboard_metrics
# -------------------------------------------------------------------


class TestListDashboardMetrics:
    def test_formats_metrics_with_changes(self):
        snap = _make_snapshot([100.0] * 7 + [80.0] * 7)
        dm = _make_dm("builtin:ga.sessions", snap)
        _set_context(_setup_context(dashboard_metrics=[dm]))
        try:
            result = agentic.list_dashboard_metrics()
            assert "builtin:ga.sessions" in result
            assert "7d:" in result
        finally:
            _clear_context()

    def test_flags_significant_drops(self):
        snap = _make_snapshot([100.0] * 7 + [80.0] * 7)
        dm = _make_dm("builtin:ga.sessions", snap)
        _set_context(_setup_context(dashboard_metrics=[dm]))
        try:
            result = agentic.list_dashboard_metrics()
            assert "[!]" in result
        finally:
            _clear_context()

    def test_no_flag_for_stable_metric(self):
        snap = _make_snapshot([100.0] * 14)
        dm = _make_dm("builtin:ga.sessions", snap)
        _set_context(_setup_context(dashboard_metrics=[dm]))
        try:
            result = agentic.list_dashboard_metrics()
            assert "[!]" not in result
        finally:
            _clear_context()

    def test_handles_no_snapshot(self):
        dm = _make_dm("builtin:ga.sessions", snapshot=None)
        _set_context(_setup_context(dashboard_metrics=[dm]))
        try:
            result = agentic.list_dashboard_metrics()
            assert "no snapshot" in result
        finally:
            _clear_context()

    def test_handles_empty_dashboard(self):
        _set_context(_setup_context(dashboard_metrics=[]))
        try:
            result = agentic.list_dashboard_metrics()
            assert "No dashboard metrics" in result
        finally:
            _clear_context()

    def test_returns_error_without_context(self):
        _clear_context()
        result = agentic.list_dashboard_metrics()
        assert "Error" in result


# -------------------------------------------------------------------
# get_metric_detail
# -------------------------------------------------------------------


class TestGetMetricDetail:
    def test_returns_metric_info(self):
        snap = _make_snapshot([50.0] * 30)
        dm = _make_dm("m1", snap)
        metric = _make_metric(
            "m1", "Sessions",
            dimensions=[MetricDimension(key="channel", name="Channel")],
        )
        _set_context(_setup_context(
            dashboard_metrics=[dm], metrics={"m1": metric},
        ))
        try:
            result = agentic.get_metric_detail("m1")
            assert "Sessions" in result
            assert "channel (Channel)" in result
            assert "50.00" in result
        finally:
            _clear_context()

    def test_returns_error_for_unknown_metric(self):
        _set_context(_setup_context())
        try:
            result = agentic.get_metric_detail("unknown")
            assert "not found" in result
        finally:
            _clear_context()

    def test_includes_time_windows(self):
        snap = _make_snapshot([10.0] * 60)
        dm = _make_dm("m1", snap)
        _set_context(_setup_context(dashboard_metrics=[dm]))
        try:
            result = agentic.get_metric_detail("m1")
            assert "1d" in result
            assert "7d" in result
            assert "30d" in result
        finally:
            _clear_context()


# -------------------------------------------------------------------
# breakdown_metric
# -------------------------------------------------------------------


class TestBreakdownMetric:
    def _make_series(self) -> MetricSeriesResult:
        points: list[MetricSeriesPoint] = []
        from datetime import date, timedelta
        base = date(2026, 4, 1)
        for i in range(14):
            d = base + timedelta(days=i)
            results = [
                MetricResult(value=100.0 if i < 7 else 60.0, groups={"channel": "organic"}),
                MetricResult(value=50.0, groups={"channel": "paid"}),
            ]
            points.append(MetricSeriesPoint(date=d, results=results))
        return MetricSeriesResult(points=points)

    def test_computes_segment_changes(self):
        dm = _make_dm("m1")
        metric = _make_metric(
            "m1", dimensions=[MetricDimension(key="channel", name="Channel")],
        )
        metric.compute_series = MagicMock(return_value=self._make_series())

        ctx = _setup_context(
            dashboard_metrics=[dm], metrics={"m1": metric},
        )
        ctx._metric_context = MagicMock()
        _set_context(ctx)
        try:
            result = agentic.breakdown_metric("m1", "channel", 7)
            assert "organic" in result
            assert "paid" in result
        finally:
            _clear_context()

    def test_handles_metric_not_found(self):
        _set_context(_setup_context())
        try:
            result = agentic.breakdown_metric("unknown", "channel")
            assert "not found" in result
        finally:
            _clear_context()

    def test_handles_no_dimensions(self):
        metric = _make_metric("m1", dimensions=[])
        _set_context(_setup_context(metrics={"m1": metric}))
        try:
            result = agentic.breakdown_metric("m1", "channel")
            assert "no dimensions" in result
        finally:
            _clear_context()

    def test_handles_invalid_dimension(self):
        metric = _make_metric(
            "m1", dimensions=[MetricDimension(key="country", name="Country")],
        )
        _set_context(_setup_context(metrics={"m1": metric}))
        try:
            result = agentic.breakdown_metric("m1", "channel")
            assert "not available" in result
            assert "country" in result
        finally:
            _clear_context()

    def test_handles_compute_error(self):
        dm = _make_dm("m1")
        metric = _make_metric(
            "m1", dimensions=[MetricDimension(key="channel", name="Channel")],
        )
        metric.compute_series = MagicMock(side_effect=RuntimeError("fail"))
        ctx = _setup_context(
            dashboard_metrics=[dm], metrics={"m1": metric},
        )
        ctx._metric_context = MagicMock()
        _set_context(ctx)
        try:
            result = agentic.breakdown_metric("m1", "channel")
            assert "Error" in result
        finally:
            _clear_context()


# -------------------------------------------------------------------
# list_tables
# -------------------------------------------------------------------


class TestListTables:
    def test_formats_table_descriptors(self):
        desc = TableDescriptor(
            key="stripe.charges",
            name="Charges",
            description="Stripe charges",
            columns=[
                TableColumn(key="id", name="ID", type=ColumnType.STRING, is_primary_key=True),
                TableColumn(key="amount", name="Amount", type=ColumnType.CURRENCY),
            ],
        )
        _set_context(_setup_context(tables=[desc]))
        try:
            result = agentic.list_tables()
            assert "stripe.charges" in result
            assert "Charges" in result
            assert "id" in result
            assert "[PK]" in result
            assert "amount" in result
            assert "currency" in result
        finally:
            _clear_context()

    def test_handles_no_tables(self):
        _set_context(_setup_context(tables=[]))
        try:
            result = agentic.list_tables()
            assert "No data tables" in result
        finally:
            _clear_context()


# -------------------------------------------------------------------
# query_table
# -------------------------------------------------------------------


class TestQueryTable:
    def _make_table_context(self) -> AnalysisContext:
        from kpidebug.data.table_memory import InMemoryDataTable
        desc = TableDescriptor(
            key="charges",
            name="Charges",
            columns=[
                TableColumn(key="status", name="Status", type=ColumnType.STRING),
                TableColumn(key="amount", name="Amount", type=ColumnType.NUMBER),
            ],
        )
        rows = [
            {"status": "paid", "amount": 100.0},
            {"status": "paid", "amount": 200.0},
            {"status": "refunded", "amount": 50.0},
        ]
        table = InMemoryDataTable(desc, rows)
        ctx = _setup_context()
        ctx.get_table = lambda key: table if key == "charges" else (_ for _ in ()).throw(ValueError(f"Unknown: {key}"))
        return ctx

    def test_group_by_and_aggregate(self):
        _set_context(self._make_table_context())
        try:
            result = agentic.query_table(
                "charges",
                group_by="status",
                aggregate_field="amount",
                aggregate_method="sum",
            )
            assert "paid" in result
            assert "refunded" in result
            assert "300" in result
            assert "50" in result
        finally:
            _clear_context()

    def test_aggregate_without_group(self):
        _set_context(self._make_table_context())
        try:
            result = agentic.query_table(
                "charges",
                aggregate_field="amount",
                aggregate_method="sum",
            )
            assert "350" in result
        finally:
            _clear_context()

    def test_filter_and_rows(self):
        _set_context(self._make_table_context())
        try:
            result = agentic.query_table(
                "charges",
                filter_field="status",
                filter_operator="eq",
                filter_value="paid",
            )
            assert "paid" in result
            assert "refunded" not in result.split("\n")[-1]
        finally:
            _clear_context()

    def test_handles_unknown_table(self):
        _set_context(self._make_table_context())
        try:
            result = agentic.query_table("nonexistent")
            assert "not found" in result
        finally:
            _clear_context()

    def test_invalid_filter_operator(self):
        _set_context(self._make_table_context())
        try:
            result = agentic.query_table(
                "charges",
                filter_field="status",
                filter_operator="invalid",
                filter_value="paid",
            )
            assert "Invalid filter operator" in result
        finally:
            _clear_context()

    def test_invalid_aggregation(self):
        _set_context(self._make_table_context())
        try:
            result = agentic.query_table(
                "charges",
                group_by="status",
                aggregate_field="amount",
                aggregate_method="invalid",
            )
            assert "Invalid aggregation" in result
        finally:
            _clear_context()


# -------------------------------------------------------------------
# estimate_revenue_impact
# -------------------------------------------------------------------


class TestEstimateRevenueImpact:
    def test_computes_revenue_loss(self):
        rev_snap = _make_snapshot([1000.0] * 7 + [800.0] * 7)
        rev_dm = _make_dm(
            "builtin:stripe.gross_revenue", rev_snap,
        )
        target_snap = _make_snapshot([100.0] * 7 + [70.0] * 7)
        target_dm = _make_dm("builtin:ga.sessions", target_snap)
        _set_context(_setup_context(
            dashboard_metrics=[rev_dm, target_dm],
        ))
        try:
            result = agentic.estimate_revenue_impact(
                "builtin:ga.sessions", 7,
            )
            assert "Revenue" in result
            assert "change" in result.lower() or "Change" in result
        finally:
            _clear_context()

    def test_handles_no_revenue_metric(self):
        _set_context(_setup_context(dashboard_metrics=[]))
        try:
            result = agentic.estimate_revenue_impact("m1")
            assert "No revenue data" in result
        finally:
            _clear_context()


# -------------------------------------------------------------------
# _parse_insights
# -------------------------------------------------------------------


class TestParseInsights:
    def test_parses_valid_json_block(self):
        text = '''Here are my findings:

```json
[
  {
    "headline": "Traffic dropped 20%",
    "description": "Sessions declined sharply",
    "signals": [
      {
        "metric_id": "builtin:ga.sessions",
        "description": "Sessions down 20%",
        "value": 800.0,
        "change": -0.20,
        "period_days": 7
      }
    ],
    "actions": [
      {"description": "Check ad campaigns", "priority": "high"}
    ],
    "counterfactual": {
      "value": 200.0,
      "metric_id": "builtin:ga.sessions",
      "metric_name": "Sessions",
      "description": "200 sessions recoverable",
      "revenue_impact": {"value": 1000.0, "description": "~$1k"}
    },
    "revenue_impact": {"value": 2000.0, "description": "-$2k"},
    "confidence": {"score": 0.8, "description": "Strong signal"}
  }
]
```
'''
        insights = agentic._parse_insights(text)
        assert len(insights) == 1
        i = insights[0]
        assert i.headline == "Traffic dropped 20%"
        assert len(i.signals) == 1
        assert i.signals[0].change == -0.20
        assert len(i.actions) == 1
        assert i.actions[0].priority == Priority.HIGH
        assert i.revenue_impact.value == 2000.0
        assert i.counterfactual.value == 200.0
        assert i.confidence.score == 0.8

    def test_parses_raw_json_array(self):
        text = '[{"headline": "Test", "description": "x"}]'
        insights = agentic._parse_insights(text)
        assert len(insights) == 1
        assert insights[0].headline == "Test"

    def test_returns_empty_on_invalid_json(self):
        text = "```json\n{broken json\n```"
        assert agentic._parse_insights(text) == []

    def test_returns_empty_on_no_json(self):
        text = "I found nothing noteworthy."
        assert agentic._parse_insights(text) == []

    def test_maps_priority_strings(self):
        text = '''```json
[
  {
    "headline": "t",
    "actions": [
      {"description": "a", "priority": "high"},
      {"description": "b", "priority": "medium"},
      {"description": "c", "priority": "low"}
    ]
  }
]
```'''
        insights = agentic._parse_insights(text)
        assert insights[0].actions[0].priority == Priority.HIGH
        assert insights[0].actions[1].priority == Priority.MEDIUM
        assert insights[0].actions[2].priority == Priority.LOW

    def test_handles_missing_fields(self):
        text = '[{"headline": "Minimal"}]'
        insights = agentic._parse_insights(text)
        assert len(insights) == 1
        i = insights[0]
        assert i.headline == "Minimal"
        assert i.signals == []
        assert i.actions == []
        assert i.confidence.score == 0.0

    def test_generates_unique_ids(self):
        text = (
            '[{"headline": "a"}, {"headline": "b"}, '
            '{"headline": "c"}]'
        )
        insights = agentic._parse_insights(text)
        ids = [i.id for i in insights]
        assert len(set(ids)) == 3
        for id_ in ids:
            assert id_.startswith("agentic-")

    def test_clamps_confidence_score(self):
        text = '[{"headline": "t", "confidence": {"score": 1.5}}]'
        insights = agentic._parse_insights(text)
        assert insights[0].confidence.score == 1.0

        text = '[{"headline": "t", "confidence": {"score": -0.5}}]'
        insights = agentic._parse_insights(text)
        assert insights[0].confidence.score == 0.0


# -------------------------------------------------------------------
# AgenticAnalyzer
# -------------------------------------------------------------------


class TestAgenticAnalyzer:
    @patch("kpidebug.analysis.analyzer_agent.run_adk_agent")
    @patch("kpidebug.analysis.analyzer_agent.make_model")
    def test_returns_empty_result_on_agent_failure(
        self, mock_make_model, mock_run,
    ):
        mock_run.side_effect = RuntimeError("API unavailable")
        ctx = MagicMock(spec=AnalysisContext)

        analyzer = agentic.AgenticAnalyzer()
        result = analyzer.analyze(ctx)

        assert isinstance(result, AnalysisResult)
        assert result.insights == []

    @patch("kpidebug.analysis.analyzer_agent.run_adk_agent")
    @patch("kpidebug.analysis.analyzer_agent.make_model")
    def test_clears_context_after_run(
        self, mock_make_model, mock_run,
    ):
        mock_run.side_effect = RuntimeError("fail")
        ctx = MagicMock(spec=AnalysisContext)

        analyzer = agentic.AgenticAnalyzer()
        analyzer.analyze(ctx)

        assert agentic._context is None

    @patch("kpidebug.analysis.analyzer_agent.run_adk_agent")
    @patch("kpidebug.analysis.analyzer_agent.make_model")
    def test_clears_context_after_success(
        self, mock_make_model, mock_run,
    ):
        mock_run.return_value = "[]"
        ctx = MagicMock(spec=AnalysisContext)

        analyzer = agentic.AgenticAnalyzer()
        result = analyzer.analyze(ctx)

        assert agentic._context is None
        assert isinstance(result, AnalysisResult)

    @patch("kpidebug.analysis.analyzer_agent.run_adk_agent")
    @patch("kpidebug.analysis.analyzer_agent.make_model")
    @patch("kpidebug.analysis.analyzer_agent.Agent")
    def test_parses_agent_response_into_insights(
        self, mock_agent_cls, mock_make_model, mock_run,
    ):
        mock_run.return_value = '[{"headline": "Revenue dropped"}]'
        ctx = MagicMock(spec=AnalysisContext)

        analyzer = agentic.AgenticAnalyzer()
        result = analyzer.analyze(ctx)

        assert len(result.insights) == 1
        assert result.insights[0].headline == "Revenue dropped"
