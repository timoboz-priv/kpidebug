from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException, Query

from kpidebug.api.auth import require_project_role
from kpidebug.api.stores import get_data_source_store, get_metric_store
from kpidebug.api.routes_data_sources import make_connector
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import TableFilter
from kpidebug.management.types import ProjectMember, Role
from kpidebug.metrics.builtin_metrics import MetricComputeResult, MetricDimension
from kpidebug.metrics.compute import compute_metric
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.resolver import (
    ResolvedMetric,
    is_builtin_id,
    list_builtins_for_tables,
    resolve_metric,
)
from kpidebug.metrics.types import (
    Aggregation,
    MetricComputeInput,
    MetricDefinition,
    MetricResult,
    MetricSource,
    TimeBucket,
)

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
    data_type: str = ""
    source: str = ""
    source_id: str = ""
    time_column: str = "created"
    has_custom_compute: bool = False
    dimensions: list[MetricDimension] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class MetricComputeRequest:
    source_id: str = ""
    group_by: list[str] = dataclass_field(default_factory=list)
    aggregation: str = "sum"
    filters: list[TableFilter] = dataclass_field(default_factory=list)
    time_column: str | None = None
    time_bucket: str | None = None


@dataclass_json
@dataclass
class MetricComputeResponse:
    metric_key: str = ""
    data_type: str = ""
    results: list[MetricComputeResult] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class CreateMetricRequest:
    name: str = ""
    description: str = ""
    data_type: str = "number"
    source_id: str = ""
    table: str = ""
    computation: str = ""
    dimensions: list[str] = dataclass_field(default_factory=list)


def _to_descriptor(resolved: ResolvedMetric) -> MetricDescriptor:
    return MetricDescriptor(
        id=resolved.id,
        key=resolved.id,
        name=resolved.name,
        description=resolved.description,
        data_type=resolved.data_type,
        source=resolved.source.value,
        source_id=resolved.source_id,
        time_column=resolved.time_column,
        has_custom_compute=resolved.has_custom_compute,
        dimensions=resolved.dimensions,
    )


def _parse_aggregation(value: str) -> Aggregation:
    try:
        return Aggregation(value)
    except ValueError:
        return Aggregation.SUM


def _parse_time_bucket(value: str | None) -> TimeBucket | None:
    if value is None:
        return None
    try:
        return TimeBucket(value)
    except ValueError:
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
    descriptors: list[MetricDescriptor] = []

    if source_id:
        source = data_source_store.get_source(project_id, source_id)
        if source is not None:
            try:
                connector = make_connector(source, data_source_store)
                table_keys = {t.key for t in connector.get_tables()}
                for r in list_builtins_for_tables(source_id, table_keys):
                    descriptors.append(_to_descriptor(r))
            except Exception:
                pass

        for d in metric_store.list_for_source(project_id, source_id):
            resolved = resolve_metric(d.id, project_id, metric_store)
            if resolved:
                descriptors.append(_to_descriptor(resolved))
    else:
        sources = data_source_store.list_sources(project_id)
        for source in sources:
            try:
                connector = make_connector(source, data_source_store)
                table_keys = {t.key for t in connector.get_tables()}
                for r in list_builtins_for_tables(source.id, table_keys):
                    descriptors.append(_to_descriptor(r))
            except Exception:
                pass

        for d in metric_store.list_definitions(project_id):
            resolved = resolve_metric(d.id, project_id, metric_store)
            if resolved:
                descriptors.append(_to_descriptor(resolved))

    return descriptors


@router.post("")
def create_metric(
    project_id: str,
    body: CreateMetricRequest,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> MetricDescriptor:
    from kpidebug.metrics.types import MetricDataType
    try:
        data_type = MetricDataType(body.data_type)
    except ValueError:
        data_type = MetricDataType.NUMBER

    definition = MetricDefinition(
        project_id=project_id,
        name=body.name,
        description=body.description,
        data_type=data_type,
        source=MetricSource.EXPRESSION,
        source_id=body.source_id,
        table=body.table,
        computation=body.computation,
        dimensions=body.dimensions,
    )
    created = metric_store.create_definition(definition)
    resolved = resolve_metric(created.id, project_id, metric_store)
    if resolved is None:
        raise HTTPException(status_code=500, detail="Failed to create metric")
    return _to_descriptor(resolved)


@router.get("/{metric_id}")
def get_metric(
    project_id: str,
    metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> MetricDescriptor:
    resolved = resolve_metric(metric_id, project_id, metric_store)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    return _to_descriptor(resolved)


@router.delete("/{metric_id}")
def delete_metric(
    project_id: str,
    metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> dict[str, bool]:
    if is_builtin_id(metric_id):
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
    resolved = resolve_metric(metric_id, project_id, metric_store)
    if resolved is None:
        raise HTTPException(status_code=404, detail="Metric not found")

    source_id = body.source_id or resolved.source_id
    if not source_id:
        raise HTTPException(
            status_code=400, detail="source_id is required",
        )

    source = data_source_store.get_source(project_id, source_id)
    if source is None:
        raise HTTPException(status_code=404, detail="Data source not found")

    connector = make_connector(source, data_source_store)
    rows = connector.fetch_all_rows(resolved.table)

    compute_input = MetricComputeInput(
        rows=rows,
        source_id=source_id,
        group_by=body.group_by,
        aggregation=_parse_aggregation(body.aggregation),
        filters=body.filters,
        time_column=body.time_column,
        time_bucket=_parse_time_bucket(body.time_bucket),
    )

    results = compute_metric(resolved, compute_input)

    return MetricComputeResponse(
        metric_key=resolved.id,
        data_type=resolved.data_type,
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
) -> list[MetricResult]:
    if is_builtin_id(metric_id):
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
) -> MetricResult:
    if is_builtin_id(metric_id):
        raise HTTPException(status_code=404, detail="No results for builtin metrics")
    definition = metric_store.get_definition(project_id, metric_id)
    if definition is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    result = metric_store.get_latest_result(project_id, metric_id)
    if result is None:
        raise HTTPException(status_code=404, detail="No results found")
    return result
