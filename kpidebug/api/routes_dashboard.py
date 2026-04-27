import logging
from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import require_project_role
from kpidebug.api.stores import get_dashboard_store
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.types import DashboardMetric
import kpidebug.metrics.registry as registry
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
    value_1d: float = 0.0
    value_3d: float = 0.0
    value_7d: float = 0.0
    value_30d: float = 0.0
    sparkline: list[SparklinePoint] = dataclass_field(default_factory=list)
    change_1d: float = 0.0
    change_3d: float = 0.0
    change_7d: float = 0.0
    change_30d: float = 0.0


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
    if body.metric_id.startswith("builtin:"):
        if registry.get(body.metric_id) is None:
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


@router.get("/metrics/compute")
def compute_dashboard_metrics(
    project_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    dashboard_store: AbstractDashboardStore = Depends(get_dashboard_store),
) -> DashboardComputeResponse:
    pinned = dashboard_store.list_metrics(project_id)
    if not pinned:
        return DashboardComputeResponse()

    results: list[DashboardMetricData] = []
    for dm in pinned:
        snapshot = dm.snapshot
        metric = registry.get(dm.metric_id)

        name = metric.name if metric else dm.metric_id
        description = metric.description if metric else ""
        data_type = metric.data_type if metric else "number"
        metric_key = metric.id if metric else dm.metric_id

        if snapshot is None:
            results.append(DashboardMetricData(
                dashboard_metric_id=dm.id,
                metric_id=dm.metric_id,
                metric_key=metric_key,
                name=name,
                description=description,
                data_type=data_type,
            ))
            continue

        sparkline = []
        from datetime import timedelta
        for i, val in enumerate(snapshot.values):
            day = snapshot.date - timedelta(days=len(snapshot.values) - 1 - i)
            sparkline.append(SparklinePoint(
                date=day.isoformat(),
                value=val,
            ))

        results.append(DashboardMetricData(
            dashboard_metric_id=dm.id,
            metric_id=dm.metric_id,
            metric_key=metric_key,
            name=name,
            description=description,
            data_type=data_type,
            current_value=snapshot.value,
            value_1d=snapshot.aggregate_value(1),
            value_3d=snapshot.aggregate_value(3),
            value_7d=snapshot.aggregate_value(7),
            value_30d=snapshot.aggregate_value(30),
            sparkline=sparkline,
            change_1d=snapshot.change(1),
            change_3d=snapshot.change(3),
            change_7d=snapshot.change(7),
            change_30d=snapshot.change(30),
        ))

    return DashboardComputeResponse(metrics=results)
