from kpidebug.metrics.types import DataRecord, DimensionValue
from kpidebug.metrics.registry import MetricRegistry


class TestMetricRegistry:
    def test_register_and_get(self):
        reg = MetricRegistry()

        @reg.register("test.metric")
        def compute_test(records: list[DataRecord], dimensions: list[DimensionValue]) -> float:
            return sum(r.value for r in records)

        fn = reg.get("test.metric")
        assert fn is not None
        records = [DataRecord(value=10.0), DataRecord(value=20.0)]
        assert fn(records, []) == 30.0

    def test_get_unknown_returns_none(self):
        reg = MetricRegistry()
        assert reg.get("nonexistent") is None

    def test_list_keys(self):
        reg = MetricRegistry()

        @reg.register("a.metric")
        def compute_a(records: list[DataRecord], dimensions: list[DimensionValue]) -> float:
            return 0.0

        @reg.register("b.metric")
        def compute_b(records: list[DataRecord], dimensions: list[DimensionValue]) -> float:
            return 0.0

        keys = reg.list_keys()
        assert "a.metric" in keys
        assert "b.metric" in keys
        assert len(keys) == 2
