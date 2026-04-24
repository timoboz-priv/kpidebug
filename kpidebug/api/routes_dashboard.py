import logging
from dataclasses import dataclass, field as dataclass_field
from datetime import datetime, timedelta, timezone

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import (
    get_dashboard_store,
    get_data_source_store,
    require_project_role,
)
from kpidebug.data.cached_connector import CachedConnector
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.metrics.builtin_metrics import builtin_registry
from kpidebug.metrics.dashboard_store_postgres import PostgresDashboardStore
from kpidebug.metrics.types import DashboardMetric
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
    source_id: str = ""
    metric_key: str = ""


@dataclass_json
@dataclass
class SparklinePoint:
    date: str = ""
    value: float = 0.0


@dataclass_json
@dataclass
class DashboardMetricData:
    dashboard_metric_id: str = ""
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


# --- Connector factory (shared with routes_data_sources) ---

def _make_connector(
    source, data_source_store: PostgresDataSourceStore,
) -> CachedConnector:
    from kpidebug.api.routes_data_sources import make_connector
    return make_connector(source, data_source_store)


# --- Endpoints ---

@router.get("/metrics")
def list_dashboard_metrics(
    project_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    dashboard_store: PostgresDashboardStore = Depends(get_dashboard_store),
) -> list[DashboardMetric]:
    return dashboard_store.list_metrics(project_id)


@router.post("/metrics")
def add_dashboard_metric(
    project_id: str,
    body: AddDashboardMetricRequest,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    dashboard_store: PostgresDashboardStore = Depends(get_dashboard_store),
) -> DashboardMetric:
    if not body.source_id or not body.metric_key:
        raise HTTPException(
            status_code=400, detail="source_id and metric_key are required",
        )
    try:
        return dashboard_store.add_metric(
            project_id, body.source_id, body.metric_key,
        )
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
    dashboard_store: PostgresDashboardStore = Depends(get_dashboard_store),
) -> dict[str, str]:
    dashboard_store.remove_metric(project_id, dashboard_metric_id)
    return {"status": "ok"}


@router.post("/metrics/compute")
def compute_dashboard_metrics(
    project_id: str,
    body: DashboardComputeRequest,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    dashboard_store: PostgresDashboardStore = Depends(get_dashboard_store),
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
) -> DashboardComputeResponse:
    pinned = dashboard_store.list_metrics(project_id)
    if not pinned:
        return DashboardComputeResponse()

    period_days = body.period_days if body.period_days in (7, 30, 90) else 30
    cutoff = datetime.now(timezone.utc) - timedelta(days=period_days)
    time_filter_value = cutoff.isoformat()

    from kpidebug.data.types import TableFilter
    time_filters = [TableFilter(column="created", operator="gte", value=time_filter_value)]

    source_cache: dict[str, tuple] = {}

    results: list[DashboardMetricData] = []
    for dm in pinned:
        if dm.source_id not in source_cache:
            source = data_source_store.get_source(project_id, dm.source_id)
            if source is None:
                continue
            connector = _make_connector(source, data_source_store)
            source_cache[dm.source_id] = (source, connector)

        source, connector = source_cache[dm.source_id]
        metric = builtin_registry.get(dm.metric_key)
        if metric is None:
            continue

        try:
            all_rows = connector.fetch_all_rows(metric.table)
        except Exception:
            logger.warning(
                "Failed to fetch rows for %s/%s", dm.source_id, dm.metric_key,
            )
            continue

        current_results = builtin_registry.compute(
            dm.metric_key, all_rows,
            filters=time_filters,
        )
        current_value = current_results[0].value if current_results else 0.0

        sparkline_results = builtin_registry.compute(
            dm.metric_key, all_rows,
            filters=time_filters,
            time_column="created",
            time_bucket="day",
        )
        sparkline = [
            SparklinePoint(
                date=r.groups.get("created", ""),
                value=r.value,
            )
            for r in sparkline_results
            if r.groups.get("created")
        ]

        results.append(DashboardMetricData(
            dashboard_metric_id=dm.id,
            source_id=dm.source_id,
            source_name=source.name,
            metric_key=dm.metric_key,
            name=metric.name,
            description=metric.description,
            data_type=metric.data_type,
            current_value=current_value,
            sparkline=sparkline,
        ))

    return DashboardComputeResponse(metrics=results)
