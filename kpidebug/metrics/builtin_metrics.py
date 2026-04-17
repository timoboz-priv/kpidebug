from dataclasses import dataclass, field as dataclass_field
from datetime import datetime
from typing import Callable

from dataclasses_json import dataclass_json

from kpidebug.data.types import TableFilter


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
    row_filter: Callable[[dict], bool] | None = dataclass_field(
        default=None, repr=False,
    )
    compute_fn: Callable[[list[dict]], float] | None = dataclass_field(
        default=None, repr=False,
    )


@dataclass_json
@dataclass
class MetricComputeResult:
    value: float = 0.0
    groups: dict[str, str] = dataclass_field(default_factory=dict)


AGGREGATION_FUNCTIONS = {"sum", "avg", "min", "max", "count"}


def _aggregate(rows: list[dict], field: str, method: str) -> float:
    if method == "count":
        return float(len(rows))
    values = [float(r.get(field, 0) or 0) for r in rows]
    if not values:
        return 0.0
    if method == "sum":
        return sum(values)
    elif method == "avg":
        return sum(values) / len(values)
    elif method == "min":
        return min(values)
    elif method == "max":
        return max(values)
    return 0.0


def _matches_filter(row: dict, f: TableFilter) -> bool:
    val = str(row.get(f.column, ""))
    target = f.value
    if f.operator == "eq":
        return val == target
    elif f.operator == "neq":
        return val != target
    elif f.operator == "contains":
        return target.lower() in val.lower()
    elif f.operator in ("gt", "gte", "lt", "lte"):
        try:
            fval, ftarget = float(val), float(target)
        except ValueError:
            fval, ftarget = None, None
        if fval is not None and ftarget is not None:
            if f.operator == "gt":
                return fval > ftarget
            elif f.operator == "gte":
                return fval >= ftarget
            elif f.operator == "lt":
                return fval < ftarget
            elif f.operator == "lte":
                return fval <= ftarget
        return val > target if f.operator in ("gt", "gte") else val < target
    return True


def _truncate_to_bucket(iso_str: str, bucket: str) -> str:
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        return iso_str
    if bucket == "day":
        return dt.strftime("%Y-%m-%d")
    elif bucket == "week":
        from datetime import timedelta
        start = dt - timedelta(days=dt.weekday())
        return start.strftime("%Y-%m-%d")
    elif bucket == "month":
        return dt.strftime("%Y-%m")
    elif bucket == "year":
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

    def compute(
        self,
        key: str,
        rows: list[dict],
        group_by: list[str] | None = None,
        aggregation: str = "sum",
        filters: list[TableFilter] | None = None,
        time_column: str | None = None,
        time_bucket: str | None = None,
    ) -> list[MetricComputeResult]:
        metric = self._metrics.get(key)
        if metric is None:
            raise ValueError(f"Unknown metric: {key}")

        if metric.row_filter:
            rows = [r for r in rows if metric.row_filter(r)]

        if filters:
            for f in filters:
                rows = [r for r in rows if _matches_filter(r, f)]

        dims = list(group_by or [])
        if time_column and time_bucket:
            for row in rows:
                raw = row.get(time_column, "")
                row[f"_tb_{time_column}"] = _truncate_to_bucket(
                    str(raw), time_bucket,
                )
            time_dim_key = f"_tb_{time_column}"
            if time_dim_key not in dims:
                dims.insert(0, time_dim_key)

        if not dims:
            value = (
                metric.compute_fn(rows)
                if metric.compute_fn
                else _aggregate(rows, metric.value_field, aggregation)
            )
            return [MetricComputeResult(value=value)]

        groups: dict[tuple[tuple[str, str], ...], list[dict]] = {}
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
            group_dict = {}
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


# --- Stripe metrics ---

def _sum_field(rows: list[dict], field: str) -> float:
    return sum(float(r.get(field, 0) or 0) for r in rows)


builtin_registry.register(BuiltinMetric(
    key="stripe.gross_revenue",
    name="Gross Revenue",
    description="Total charge amount for successful payments",
    table="charges",
    data_type="currency",
    value_field="amount",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="payment_method_type", name="Payment Method"),
        MetricDimension(key="card_brand", name="Card Brand"),
        MetricDimension(key="status", name="Status"),
    ],
    row_filter=lambda r: r.get("paid") is True or r.get("paid") == "true",
))

builtin_registry.register(BuiltinMetric(
    key="stripe.net_revenue",
    name="Net Revenue",
    description="Revenue after Stripe fees (from balance transactions)",
    table="balance_transactions",
    data_type="currency",
    value_field="net",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="reporting_category", name="Reporting Category"),
    ],
    row_filter=lambda r: r.get("type") == "charge",
))

builtin_registry.register(BuiltinMetric(
    key="stripe.customer_count",
    name="Customer Count",
    description="Total number of customers",
    table="customers",
    data_type="number",
    value_field="id",
    dimensions=[
        MetricDimension(key="country", name="Country"),
        MetricDimension(key="delinquent", name="Delinquent"),
    ],
    compute_fn=lambda rows: float(len(rows)),
))

builtin_registry.register(BuiltinMetric(
    key="stripe.mrr",
    name="Monthly Recurring Revenue",
    description="Sum of active subscription amounts (monthly basis)",
    table="subscriptions",
    data_type="currency",
    value_field="amount",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
        MetricDimension(key="interval", name="Billing Interval"),
    ],
    row_filter=lambda r: r.get("status") == "active",
))

builtin_registry.register(BuiltinMetric(
    key="stripe.refund_rate",
    name="Refund Rate",
    description="Percentage of charge amount that was refunded",
    table="charges",
    data_type="percent",
    value_field="amount_refunded",
    dimensions=[
        MetricDimension(key="currency", name="Currency"),
    ],
    compute_fn=lambda rows: (
        (_sum_field(rows, "amount_refunded") / _sum_field(rows, "amount") * 100)
        if _sum_field(rows, "amount") > 0 else 0.0
    ),
    row_filter=lambda r: r.get("paid") is True or r.get("paid") == "true",
))
