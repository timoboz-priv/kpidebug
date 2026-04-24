from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from typing import Callable

from dataclasses_json import dataclass_json

from kpidebug.data.types import Row
from kpidebug.metrics.filters import apply_filters
from kpidebug.metrics.types import Aggregation, MetricComputeInput, TimeBucket


@dataclass_json
@dataclass
class MetricDimension:
    key: str = ""
    name: str = ""


@dataclass_json
@dataclass
class BuiltinMetric:
    key: str = ""
    name: str = ""
    description: str = ""
    table: str = ""
    data_type: str = "number"
    value_field: str = ""
    dimensions: list[MetricDimension] = dataclass_field(default_factory=list)
    row_filter: Callable[[Row], bool] | None = dataclass_field(
        default=None, repr=False,
    )
    compute_fn: Callable[[list[Row]], float] | None = dataclass_field(
        default=None, repr=False,
    )

    @property
    def id(self) -> str:
        return f"builtin:{self.key}"


@dataclass_json
@dataclass
class MetricComputeResult:
    value: float = 0.0
    groups: dict[str, str] = dataclass_field(default_factory=dict)


def _aggregate(rows: list[Row], field: str, method: Aggregation) -> float:
    if method == Aggregation.COUNT:
        return float(len(rows))
    values = [float(r.get(field, 0) or 0) for r in rows]
    if not values:
        return 0.0
    if method == Aggregation.SUM:
        return sum(values)
    elif method == Aggregation.AVG:
        return sum(values) / len(values)
    elif method == Aggregation.MIN:
        return min(values)
    elif method == Aggregation.MAX:
        return max(values)
    return 0.0


def _truncate_to_bucket(iso_str: str, bucket: TimeBucket) -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return iso_str
    if bucket == TimeBucket.DAY:
        return dt.strftime("%Y-%m-%d")
    elif bucket == TimeBucket.WEEK:
        from datetime import timedelta
        start = dt - timedelta(days=dt.weekday())
        return start.strftime("%Y-%m-%d")
    elif bucket == TimeBucket.MONTH:
        return dt.strftime("%Y-%m")
    elif bucket == TimeBucket.YEAR:
        return dt.strftime("%Y")
    return iso_str


class BuiltinMetricRegistry:
    _metrics: dict[str, BuiltinMetric]

    def __init__(self):
        self._metrics = {}

    def register(self, metric: BuiltinMetric) -> None:
        self._metrics[metric.key] = metric

    def get(self, key: str) -> BuiltinMetric | None:
        return self._metrics.get(key)

    def list_all(self) -> list[BuiltinMetric]:
        return list(self._metrics.values())

    def list_for_table(self, table: str) -> list[BuiltinMetric]:
        return [m for m in self._metrics.values() if m.table == table]

    def list_for_tables(self, table_keys: set[str]) -> list[BuiltinMetric]:
        return [m for m in self._metrics.values() if m.table in table_keys]

    def compute(
        self,
        key: str,
        compute_input: MetricComputeInput,
    ) -> list[MetricComputeResult]:
        metric = self._metrics.get(key)
        if metric is None:
            raise ValueError(f"Unknown metric: {key}")

        rows = list(compute_input.rows)

        if metric.row_filter:
            rows = [r for r in rows if metric.row_filter(r)]

        rows = apply_filters(rows, compute_input.filters)

        dims = list(compute_input.group_by)
        time_column = compute_input.time_column
        time_bucket = compute_input.time_bucket
        if time_column and time_bucket:
            for row in rows:
                raw = row.get(time_column, "")
                row[f"_tb_{time_column}"] = _truncate_to_bucket(
                    str(raw), time_bucket,
                )
            time_dim_key = f"_tb_{time_column}"
            if time_dim_key not in dims:
                dims.insert(0, time_dim_key)

        aggregation = compute_input.aggregation

        if not dims:
            value = (
                metric.compute_fn(rows)
                if metric.compute_fn
                else _aggregate(rows, metric.value_field, aggregation)
            )
            return [MetricComputeResult(value=value)]

        groups: dict[tuple[tuple[str, str], ...], list[Row]] = {}
        for row in rows:
            key_parts = tuple(
                (d, str(row.get(d, ""))) for d in dims
            )
            groups.setdefault(key_parts, []).append(row)

        results: list[MetricComputeResult] = []
        for group_key, group_rows in sorted(groups.items()):
            value = (
                metric.compute_fn(group_rows)
                if metric.compute_fn
                else _aggregate(
                    group_rows, metric.value_field, aggregation,
                )
            )
            group_dict: dict[str, str] = {}
            for dim_key, dim_val in group_key:
                display_key = dim_key.replace(
                    f"_tb_{time_column}", time_column,
                ) if time_column else dim_key
                group_dict[display_key] = dim_val
            results.append(MetricComputeResult(
                value=value, groups=group_dict,
            ))

        return results


builtin_registry = BuiltinMetricRegistry()

import kpidebug.metrics.stripe.metrics  # noqa: E402, F401
