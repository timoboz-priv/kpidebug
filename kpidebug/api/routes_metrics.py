from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import (
    get_current_user,
    get_metric_store,
    require_project_role,
)
from kpidebug.management.types import ProjectMember, Role, User
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.types import MetricDefinition, MetricResult

router = APIRouter(
    prefix="/api/projects/{project_id}/metric-definitions",
    tags=["metric-definitions"],
)


@router.get("")
def list_metrics(
    project_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> list[MetricDefinition]:
    return metric_store.list_definitions(project_id)


@router.post("")
def create_metric(
    project_id: str,
    body: MetricDefinition,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> MetricDefinition:
    body.project_id = project_id
    return metric_store.create_definition(body)


@router.get("/{metric_id}")
def get_metric(
    project_id: str,
    metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> MetricDefinition:
    definition = metric_store.get_definition(project_id, metric_id)
    if definition is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    return definition


@router.put("/{metric_id}")
def update_metric(
    project_id: str,
    metric_id: str,
    body: MetricDefinition,
    _member: ProjectMember = Depends(require_project_role(Role.EDIT)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> MetricDefinition:
    existing = metric_store.get_definition(project_id, metric_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    return metric_store.update_definition(project_id, metric_id, {
        "name": body.name,
        "description": body.description,
        "data_type": body.data_type.value,
        "computation": body.computation,
        "source_filters": [
            {"source_type": sf.source_type.value, "fields": sf.fields}
            for sf in body.source_filters
        ],
        "dimensions": body.dimensions,
    })


@router.delete("/{metric_id}")
def delete_metric(
    project_id: str,
    metric_id: str,
    _member: ProjectMember = Depends(require_project_role(Role.ADMIN)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> dict:
    existing = metric_store.get_definition(project_id, metric_id)
    if existing is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    metric_store.delete_definition(project_id, metric_id)
    return {"ok": True}



@router.get("/{metric_id}/results")
def get_metric_results(
    project_id: str,
    metric_id: str,
    start_time: str | None = None,
    end_time: str | None = None,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> list[MetricResult]:
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
    definition = metric_store.get_definition(project_id, metric_id)
    if definition is None:
        raise HTTPException(status_code=404, detail="Metric not found")
    result = metric_store.get_latest_result(project_id, metric_id)
    if result is None:
        raise HTTPException(status_code=404, detail="No results found")
    return result
