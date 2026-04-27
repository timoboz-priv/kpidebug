from kpidebug.metrics.types import Metric

_metrics: dict[str, Metric] = {}


def register(metric: Metric) -> None:
    _metrics[metric.id] = metric


def get(metric_id: str) -> Metric | None:
    return _metrics.get(metric_id)


def list_all() -> list[Metric]:
    return list(_metrics.values())


def list_for_tables(table_keys: set[str]) -> list[Metric]:
    return [m for m in _metrics.values() if set(m.table_keys) & table_keys]


import kpidebug.metrics.stripe.metrics  # noqa: E402, F401
import kpidebug.metrics.google_analytics.metrics  # noqa: E402, F401
