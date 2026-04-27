import logging
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timedelta, timezone

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import require_project_role
from kpidebug.api.stores import get_dashboard_store, get_data_source_store
from kpidebug.api.routes_data_sources import make_connector
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import TableFilter
from kpidebug.metrics.builtin_metrics import MetricComputeResult
from kpidebug.metrics.compute import compute_metric
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.resolver import is_builtin_id, resolve_builtin
from kpidebug.metrics.types import (
    Aggregation,
    DashboardMetric,
    MetricComputeInput,
    SourceConnectorPair,
    TimeBucket,
)
from kpidebug.management.types import ProjectMember, Role

logger = logging.getLogger(__name__)

router = APIRouter(
    prefix="/api/projects/{project_id}/dashboard",
    tags=["dashboard"],
)


# --- Request / Response types ---

@dataclass_json
@dataclass
class AddDashboardMetricRequest:
    metric_id: str = ""


@dataclass_json
@dataclass
class SparklinePoint:
    date: str = ""
    value: float = 0.0


@dataclass_json
@dataclass
class DashboardMetricData:
    dashboard_metric_id: str = ""
    metric_id: str = ""
    source_id: str = ""
    source_name: str = ""
    metric_key: str = ""
    name: str = ""
    description: str = ""
    data_type: str = ""
    current_value: float = 0.0
    sparkline: list[SparklinePoint] = dataclass_field(default_factory=list)


@dataclass_json
@dataclass
class DashboardComputeRequest:
    period_days: int = 30


@dataclass_json
@dataclass
class DashboardComputeResponse:
    metrics: list[DashboardMetricData] = dataclass_field(default_factory=list)


# --- Endpoints ---

@router.get("/metrics")
def list_dashboard_metrics(
    project_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    dashboard_store: AbstractDashboardStore = Depends(get_dashboard_store),
) -> list[DashboardMetric]:
    return dashboard_store.list_metrics(project_id)


@router.post("/metrics")
def add_dashboard_metric(
    project_id: str,
    body: AddDashboardMetricRequest,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    dashboard_store: AbstractDashboardStore = Depends(get_dashboard_store),
) -> DashboardMetric:
    if not body.metric_id:
        raise HTTPException(
            status_code=400, detail="metric_id is required",
        )
    if is_builtin_id(body.metric_id):
        resolved = resolve_builtin(body.metric_id)
        if resolved is None:
            raise HTTPException(status_code=404, detail="Metric not found")
    try:
        return dashboard_store.add_metric(project_id, body.metric_id)
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(
                status_code=409, detail="Metric already on dashboard",
            )
        raise


@router.delete("/metrics/{dashboard_metric_id}")
def remove_dashboard_metric(
    project_id: str,
    dashboard_metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    dashboard_store: AbstractDashboardStore = Depends(get_dashboard_store),
) -> dict[str, str]:
    dashboard_store.remove_metric(project_id, dashboard_metric_id)
    return {"status": "ok"}


@router.post("/metrics/compute")
def compute_dashboard_metrics(
    project_id: str,
    body: DashboardComputeRequest,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    dashboard_store: AbstractDashboardStore = Depends(get_dashboard_store),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> DashboardComputeResponse:
    pinned = dashboard_store.list_metrics(project_id)
    if not pinned:
        return DashboardComputeResponse()

    period_days = body.period_days if body.period_days in (7, 30, 90) else 30
    cutoff = datetime.now(timezone.utc) - timedelta(days=period_days)
    time_filter_value = cutoff.isoformat()
    source_cache: dict[str, SourceConnectorPair] = {}
    _warm_source_cache(project_id, data_source_store, source_cache)

    results: list[DashboardMetricData] = []
    for dm in pinned:
        resolved = resolve_builtin(dm.metric_id) if is_builtin_id(dm.metric_id) else None
        if resolved is None:
            continue

        pair = _find_source_for_table(resolved.table, source_cache)
        if pair is None:
            continue

        try:
            all_rows = pair.connector.fetch_all_rows(resolved.table)
        except Exception:
            logger.warning("Failed to fetch rows for metric %s", dm.metric_id)
            continue

        time_col = resolved.time_column
        metric_time_filters = [
            TableFilter(column=time_col, operator="gte", value=time_filter_value),
        ]

        current_input = MetricComputeInput(
            rows=all_rows, filters=metric_time_filters,
        )
        current_results = compute_metric(resolved, current_input)
        current_value = current_results[0].value if current_results else 0.0

        sparkline_input = MetricComputeInput(
            rows=all_rows,
            filters=metric_time_filters,
            time_column=time_col,
            time_bucket=TimeBucket.DAY,
        )
        sparkline_results = compute_metric(resolved, sparkline_input)
        sparkline = [
            SparklinePoint(
                date=r.groups.get(time_col, ""),
                value=r.value,
            )
            for r in sparkline_results
            if r.groups.get(time_col)
        ]

        results.append(DashboardMetricData(
            dashboard_metric_id=dm.id,
            metric_id=dm.metric_id,
            source_id=pair.source.id,
            source_name=pair.source.name,
            metric_key=resolved.id,
            name=resolved.name,
            description=resolved.description,
            data_type=resolved.data_type,
            current_value=current_value,
            sparkline=sparkline,
        ))

    return DashboardComputeResponse(metrics=results)


def _warm_source_cache(
    project_id: str,
    data_source_store: PostgresDataSourceStore,
    cache: dict[str, SourceConnectorPair],
) -> None:
    for source in data_source_store.list_sources(project_id):
        try:
            connector = make_connector(source, data_source_store)
            cache[source.id] = SourceConnectorPair(source=source, connector=connector)
        except Exception:
            continue


def _find_source_for_table(
    table: str,
    cache: dict[str, SourceConnectorPair],
) -> SourceConnectorPair | None:
    for pair in cache.values():
        try:
            table_keys = {t.key for t in pair.connector.get_tables()}
            if table in table_keys:
                return pair
        except Exception:
            continue
    return None
