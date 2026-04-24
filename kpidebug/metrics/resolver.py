from __future__ import annotations

from dataclasses import dataclass, field as dataclass_field

from kpidebug.metrics.builtin_metrics import (
    BuiltinMetric,
    MetricDimension,
    builtin_registry,
)
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.types import MetricDefinition, MetricSource

BUILTIN_ID_PREFIX = "builtin:"


def is_builtin_id(metric_id: str) -> bool:
    return metric_id.startswith(BUILTIN_ID_PREFIX)


def parse_builtin_key(metric_id: str) -> str:
    return metric_id[len(BUILTIN_ID_PREFIX):]


@dataclass
class ResolvedMetric:
    id: str = ""
    name: str = ""
    description: str = ""
    data_type: str = ""
    source: MetricSource = MetricSource.BUILTIN
    source_id: str = ""
    table: str = ""
    time_column: str = "created"
    dimensions: list[MetricDimension] = dataclass_field(default_factory=list)
    has_custom_compute: bool = False
    builtin: BuiltinMetric | None = None
    definition: MetricDefinition | None = None


def resolve_metric(
    metric_id: str,
    project_id: str,
    metric_store: AbstractMetricStore,
) -> ResolvedMetric | None:
    if is_builtin_id(metric_id):
        return resolve_builtin(metric_id)

    definition = metric_store.get_definition(project_id, metric_id)
    if definition is None:
        return None
    return _from_definition(definition)


def resolve_builtin(metric_id: str, source_id: str = "") -> ResolvedMetric | None:
    builtin_key = parse_builtin_key(metric_id)
    builtin = builtin_registry.get(builtin_key)
    if builtin is None:
        return None
    return ResolvedMetric(
        id=builtin.id,
        name=builtin.name,
        description=builtin.description,
        data_type=builtin.data_type,
        source=MetricSource.BUILTIN,
        source_id=source_id,
        table=builtin.table,
        time_column=builtin.time_column,
        dimensions=builtin.dimensions,
        has_custom_compute=builtin.compute_fn is not None,
        builtin=builtin,
    )


def _from_definition(definition: MetricDefinition) -> ResolvedMetric:
    return ResolvedMetric(
        id=definition.id,
        name=definition.name,
        description=definition.description,
        data_type=definition.data_type.value,
        source=definition.source,
        source_id=definition.source_id,
        table=definition.table,
        dimensions=[MetricDimension(key=d, name=d) for d in definition.dimensions],
        has_custom_compute=False,
        definition=definition,
    )


def list_builtins_for_tables(
    source_id: str, table_keys: set[str],
) -> list[ResolvedMetric]:
    results: list[ResolvedMetric] = []
    for m in builtin_registry.list_for_tables(table_keys):
        results.append(ResolvedMetric(
            id=m.id,
            name=m.name,
            description=m.description,
            data_type=m.data_type,
            source=MetricSource.BUILTIN,
            source_id=source_id,
            table=m.table,
            time_column=m.time_column,
            dimensions=m.dimensions,
            has_custom_compute=m.compute_fn is not None,
            builtin=m,
        ))
    return results
