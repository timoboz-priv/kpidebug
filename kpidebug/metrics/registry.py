from typing import Callable

from kpidebug.data.types import DataRecord, DimensionValue

BuiltinMetricFn = Callable[[list[DataRecord], list[DimensionValue]], float]


class MetricRegistry:
    _metrics: dict[str, BuiltinMetricFn]

    def __init__(self):
        self._metrics = {}

    def register(self, key: str) -> Callable[[BuiltinMetricFn], BuiltinMetricFn]:
        def decorator(fn: BuiltinMetricFn) -> BuiltinMetricFn:
            self._metrics[key] = fn
            return fn
        return decorator

    def get(self, key: str) -> BuiltinMetricFn | None:
        return self._metrics.get(key)

    def list_keys(self) -> list[str]:
        return list(self._metrics.keys())


registry = MetricRegistry()
