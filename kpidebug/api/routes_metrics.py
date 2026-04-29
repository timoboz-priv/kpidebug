from dataclasses import dataclass, field as dataclass_field
from datetime import date

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException, Query

from kpidebug.api.auth import require_project_role
from kpidebug.api.stores import get_data_source_store, get_metric_store
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import Aggregation, TableFilter
from kpidebug.management.types import ProjectMember, Role
from kpidebug.metrics.context import MetricContext
from kpidebug.metrics.expression_metric import ExpressionMetric
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.types import (
    Metric,
    MetricDataType,
    MetricDefinition,
    MetricDimension,
    MetricResult,
    StoredMetricResult,
)
import kpidebug.metrics.registry as registry

router = APIRouter(
    prefix="/api/projects/{project_id}/metrics",
    tags=["metrics"],
)


# --- Request / Response types ---

@dataclass_json
@dataclass
class MetricDescriptor:
    id: str = ""
    key: str = ""
    name: str = ""
    description: str = ""
    data_type: MetricDataType = MetricDataType.NUMBER
    source_type: str = "custom"
    default_aggregation: str = "sum"
    dimensions: list[MetricDimension] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class MetricComputeRequest:
    group_by: list[str] = dataclass_field(default_factory=list)
    aggregation: str = "sum"
    filters: list[TableFilter] = dataclass_field(default_factory=list)
    time_column: str | None = None
    time_bucket: str | None = None


@dataclass_json
@dataclass
class MetricComputeResponse:
    metric_key: str = ""
    data_type: MetricDataType = MetricDataType.NUMBER
    results: list[MetricResult] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class CreateMetricRequest:
    name: str = ""
    description: str = ""
    data_type: MetricDataType = MetricDataType.NUMBER
    computation: str = ""


def _extract_time_range(
    filters: list[TableFilter] | None,
) -> tuple[int, date | None, list[TableFilter]]:
    from datetime import datetime as dt, timezone
    if not filters:
        return 30, None, []
    now = dt.now(timezone.utc)
    start: dt | None = None
    end: dt | None = None
    non_time: list[TableFilter] = []
    for f in filters:
        if f.operator in ("gte", "gt") and f.value and "T" in f.value:
            try:
                start = dt.fromisoformat(f.value.replace("Z", "+00:00"))
            except ValueError:
                non_time.append(f)
        elif f.operator in ("lte", "lt") and f.value and "T" in f.value:
            try:
                end = dt.fromisoformat(f.value.replace("Z", "+00:00"))
            except ValueError:
                non_time.append(f)
        else:
            non_time.append(f)
    end_ref = end or now
    if start:
        days = max(1, (end_ref - start).days)
    else:
        days = 30
    end_date = end.date() if end else None
    return days, end_date, non_time


def _to_descriptor(metric: Metric) -> MetricDescriptor:
    return MetricDescriptor(
        id=metric.id,
        key=metric.id,
        name=metric.name,
        description=metric.description,
        data_type=metric.data_type,
        source_type=metric.source_type.value,
        default_aggregation=metric.default_aggregation.value,
        dimensions=metric.dimensions,
    )


def _def_to_descriptor(d: MetricDefinition) -> MetricDescriptor:
    return MetricDescriptor(
        id=d.id,
        key=d.id,
        name=d.name,
        description=d.description,
        data_type=d.data_type,
    )


def _resolve(metric_id: str, project_id: str, metric_store: AbstractMetricStore) -> Metric | None:
    builtin = registry.get(metric_id)
    if builtin is not None:
        return builtin
    definition = metric_store.get_definition(project_id, metric_id)
    if definition is not None:
        return ExpressionMetric(definition)
    return None


# --- List / CRUD ---

@router.get("")
def list_metrics(
    project_id: str,
    source_id: str | None = Query(None),
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> list[MetricDescriptor]:
    from kpidebug.api.routes_data_sources import make_connector
    descriptors: list[MetricDescriptor] = []

    if source_id:
        source = data_source_store.get_source(project_id, source_id)
        if source is not None:
            try:
                connector = make_connector(source, data_source_store)
                table_keys = {t.key for t in connector.get_tables()}
                for m in registry.list_for_tables(table_keys):
                    descriptors.append(_to_descriptor(m))
            except Exception:
                pass
    else:
        sources = data_source_store.list_sources(project_id)
        for source in sources:
            try:
                connector = make_connector(source, data_source_store)
                table_keys = {t.key for t in connector.get_tables()}
                for m in registry.list_for_tables(table_keys):
                    descriptors.append(_to_descriptor(m))
            except Exception:
                pass

    for d in metric_store.list_definitions(project_id):
        descriptors.append(_def_to_descriptor(d))

    return descriptors


@router.post("")
def create_metric(
    project_id: str,
    body: CreateMetricRequest,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> MetricDescriptor:
    definition = MetricDefinition(
        project_id=project_id,
        name=body.name,
        description=body.description,
        data_type=body.data_type,
        computation=body.computation,
    )
    created = metric_store.create_definition(definition)
    return _def_to_descriptor(created)


@router.get("/{metric_id}")
def get_metric(
    project_id: str,
    metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> MetricDescriptor:
    metric = _resolve(metric_id, project_id, metric_store)
    if metric is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    return _to_descriptor(metric)


@router.delete("/{metric_id}")
def delete_metric(
    project_id: str,
    metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> dict[str, bool]:
    if metric_id.startswith("builtin:"):
        raise HTTPException(status_code=400, detail="Cannot delete builtin metrics")
    existing = metric_store.get_definition(project_id, metric_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    metric_store.delete_definition(project_id, metric_id)
    return {"ok": True}


# --- Compute ---

@router.post("/{metric_id}/compute")
def compute_metric_endpoint(
    project_id: str,
    metric_id: str,
    body: MetricComputeRequest,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> MetricComputeResponse:
    metric = _resolve(metric_id, project_id, metric_store)
    if metric is None:
        raise HTTPException(status_code=404, detail="Metric not found")

    ctx = MetricContext.for_project(project_id, data_source_store)

    try:
        agg = Aggregation(body.aggregation)
    except ValueError:
        agg = Aggregation.SUM

    dimensions = body.group_by if body.group_by else None
    days, end_date, non_time_filters = _extract_time_range(body.filters)

    actual_agg = Aggregation.SUM if agg == Aggregation.AVG_DAILY else agg

    if agg == Aggregation.AVG_DAILY and not body.time_column:
        from kpidebug.common.math import aggregate_values
        series = metric.compute_series(
            ctx, dimensions=dimensions, aggregation=Aggregation.SUM,
            filters=non_time_filters or None, days=days, date=end_date,
        )
        daily_values = [p.results[0].value if p.results else 0.0 for p in series.points]
        results: list[MetricResult] = [MetricResult(value=aggregate_values(daily_values, Aggregation.AVG_DAILY))]
    elif body.time_column and body.time_bucket:
        from kpidebug.metrics.types import TimeBucket
        try:
            tb = TimeBucket(body.time_bucket)
        except ValueError:
            tb = TimeBucket.DAY
        series = metric.compute_series(
            ctx, dimensions=dimensions, aggregation=actual_agg,
            filters=non_time_filters or None, days=days,
            date=end_date, time_bucket=tb,
        )
        results = []
        for point in series.points:
            for r in point.results:
                groups = dict(r.groups)
                groups[body.time_column] = point.date.isoformat()
                results.append(MetricResult(value=r.value, groups=groups))
    else:
        results = metric.compute_single(
            ctx, dimensions=dimensions, aggregation=actual_agg,
            filters=non_time_filters or None, days=days, date=end_date,
        )

    return MetricComputeResponse(
        metric_key=metric.id,
        data_type=metric.data_type,
        results=results,
    )


# --- Stored results ---

@router.get("/{metric_id}/results")
def get_metric_results(
    project_id: str,
    metric_id: str,
    start_time: str | None = None,
    end_time: str | None = None,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> list[StoredMetricResult]:
    if metric_id.startswith("builtin:"):
        return []
    definition = metric_store.get_definition(project_id, metric_id)
    if definition is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    return metric_store.get_results(project_id, metric_id, start_time, end_time)


@router.get("/{metric_id}/results/latest")
def get_latest_metric_result(
    project_id: str,
    metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> StoredMetricResult:
    if metric_id.startswith("builtin:"):
        raise HTTPException(status_code=404, detail="No results for builtin metrics")
    definition = metric_store.get_definition(project_id, metric_id)
    if definition is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    result = metric_store.get_latest_result(project_id, metric_id)
    if result is None:
        raise HTTPException(status_code=404, detail="No results found")
    return result
