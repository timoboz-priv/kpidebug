import pytest

from kpidebug.data.types import DataRecord, DataSourceType, DimensionValue
from kpidebug.metrics.engine import MetricEngine
from kpidebug.metrics.registry import MetricRegistry, registry
from kpidebug.metrics.types import (
    MetricDataType,
    MetricDefinition,
    MetricSource,
    SourceFilter,
)


def _make_records() -> list[DataRecord]:
    return [
        DataRecord(
            source_type=DataSourceType.STRIPE,
            field="revenue", value=100.0, timestamp="2026-04-01T00:00:00Z",
            dimension_values=[DimensionValue(dimension="geo.country", value="US")],
        ),
        DataRecord(
            source_type=DataSourceType.STRIPE,
            field="revenue", value=200.0, timestamp="2026-04-02T00:00:00Z",
            dimension_values=[DimensionValue(dimension="geo.country", value="DE")],
        ),
        DataRecord(
            source_type=DataSourceType.GOOGLE_ANALYTICS,
            field="page_views", value=1000.0, timestamp="2026-04-01T00:00:00Z",
            dimension_values=[DimensionValue(dimension="geo.country", value="US")],
        ),
        DataRecord(
            source_type=DataSourceType.STRIPE,
            field="transactions", value=5.0, timestamp="2026-04-01T00:00:00Z",
            dimension_values=[DimensionValue(dimension="geo.country", value="US")],
        ),
    ]


class TestMetricEngine:
    def setup_method(self):
        self.engine = MetricEngine()

    def test_compute_ai_generated_no_dimensions(self):
        definition = MetricDefinition(
            id="m1", project_id="p1", name="Total Revenue",
            data_type=MetricDataType.CURRENCY,
            source=MetricSource.AI_GENERATED,
            computation="sum('revenue')",
            source_filters=[SourceFilter(source_type=DataSourceType.STRIPE, fields=["revenue"])],
        )
        records = _make_records()
        results = self.engine.compute(definition, records)

        assert len(results) == 1
        assert results[0].value == 300.0
        assert results[0].metric_id == "m1"
        assert results[0].project_id == "p1"

    def test_compute_with_dimension_grouping(self):
        definition = MetricDefinition(
            id="m2", project_id="p1", name="Revenue by Country",
            data_type=MetricDataType.CURRENCY,
            source=MetricSource.AI_GENERATED,
            computation="sum('revenue')",
            source_filters=[SourceFilter(source_type=DataSourceType.STRIPE, fields=["revenue"])],
            dimensions=["geo.country"],
        )
        records = _make_records()
        results = self.engine.compute(definition, records)

        assert len(results) == 2
        values_by_country = {
            r.dimension_values[0].value: r.value for r in results
        }
        assert values_by_country["US"] == 100.0
        assert values_by_country["DE"] == 200.0

    def test_source_filter_restricts_records(self):
        definition = MetricDefinition(
            id="m3", project_id="p1", name="Page Views",
            source=MetricSource.AI_GENERATED,
            computation="sum('page_views')",
            source_filters=[
                SourceFilter(source_type=DataSourceType.GOOGLE_ANALYTICS, fields=["page_views"])
            ],
        )
        records = _make_records()
        results = self.engine.compute(definition, records)

        assert len(results) == 1
        assert results[0].value == 1000.0

    def test_source_filter_by_type_only(self):
        definition = MetricDefinition(
            id="m4", project_id="p1", name="All Stripe",
            source=MetricSource.AI_GENERATED,
            computation="count('revenue') + count('transactions')",
            source_filters=[SourceFilter(source_type=DataSourceType.STRIPE)],
        )
        records = _make_records()
        results = self.engine.compute(definition, records)

        assert len(results) == 1
        # 2 revenue + 1 transactions from Stripe
        assert results[0].value == 3.0

    def test_no_source_filters_uses_all_records(self):
        definition = MetricDefinition(
            id="m5", project_id="p1", name="Everything",
            source=MetricSource.AI_GENERATED,
            computation="count('revenue') + count('page_views')",
        )
        records = _make_records()
        results = self.engine.compute(definition, records)

        assert len(results) == 1
        assert results[0].value == 3.0

    def test_compute_builtin_metric(self):
        test_registry = MetricRegistry()

        @test_registry.register("test.total_value")
        def compute_total(records: list[DataRecord], dimensions: list[DimensionValue]) -> float:
            return sum(r.value for r in records)

        # Temporarily register in the global registry
        original = registry._metrics.copy()
        registry._metrics["test.total_value"] = test_registry.get("test.total_value")

        try:
            definition = MetricDefinition(
                id="m6", project_id="p1", name="Total Value",
                source=MetricSource.BUILTIN,
                builtin_key="test.total_value",
            )
            records = _make_records()
            results = self.engine.compute(definition, records)

            assert len(results) == 1
            assert results[0].value == 1305.0  # 100 + 200 + 1000 + 5
        finally:
            registry._metrics = original

    def test_compute_derives_period(self):
        definition = MetricDefinition(
            id="m7", project_id="p1", name="Revenue",
            source=MetricSource.AI_GENERATED,
            computation="sum('revenue')",
            source_filters=[SourceFilter(source_type=DataSourceType.STRIPE, fields=["revenue"])],
        )
        records = _make_records()
        results = self.engine.compute(definition, records)

        assert results[0].period_start == "2026-04-01T00:00:00Z"
        assert results[0].period_end == "2026-04-02T00:00:00Z"

    def test_unknown_builtin_raises(self):
        definition = MetricDefinition(
            id="m8", project_id="p1", name="Unknown",
            source=MetricSource.BUILTIN,
            builtin_key="nonexistent.metric",
        )
        with pytest.raises(ValueError, match="Unknown built-in metric"):
            self.engine.compute(definition, _make_records())

    def test_ai_generated_no_computation_raises(self):
        definition = MetricDefinition(
            id="m9", project_id="p1", name="Empty",
            source=MetricSource.AI_GENERATED,
            computation="",
        )
        with pytest.raises(ValueError, match="has no computation expression"):
            self.engine.compute(definition, _make_records())

    def test_empty_records(self):
        definition = MetricDefinition(
            id="m10", project_id="p1", name="Empty",
            source=MetricSource.AI_GENERATED,
            computation="sum('revenue')",
        )
        results = self.engine.compute(definition, [])

        assert len(results) == 1
        assert results[0].value == 0.0
        assert results[0].period_start == ""
        assert results[0].period_end == ""
