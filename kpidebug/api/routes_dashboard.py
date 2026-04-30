import logging
from dataclasses import dataclass, field as dataclass_field

from dataclasses_json import dataclass_json
from fastapi import APIRouter, Depends, HTTPException

from kpidebug.api.auth import require_project_role
from kpidebug.api.stores import get_dashboard_store, get_data_source_store, get_insight_store, get_metric_store
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.data.types import Aggregation
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.types import DashboardMetric
import kpidebug.metrics.registry as registry
from kpidebug.management.types import ProjectMember, Role
from kpidebug.analysis.insight_store import AbstractInsightStore
from kpidebug.analysis.types import Insight

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
    aggregation: str = "sum"


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
    source_type: str = ""
    metric_key: str = ""
    name: str = ""
    description: str = ""
    data_type: str = ""
    aggregation: str = "sum"
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


@dataclass_json
@dataclass
class InsightSignalData:
    metric_id: str = ""
    description: str = ""
    value: float = 0.0
    change: float = 0.0
    period_days: int = 0


@dataclass_json
@dataclass
class InsightActionData:
    description: str = ""
    priority: str = "medium"


@dataclass_json
@dataclass
class RevenueImpactData:
    value: float = 0.0
    description: str = ""


@dataclass_json
@dataclass
class CounterfactualData:
    value: float = 0.0
    metric_id: str = ""
    metric_name: str = ""
    description: str = ""
    revenue_impact: RevenueImpactData = dataclass_field(default_factory=RevenueImpactData)


@dataclass_json
@dataclass
class ConfidenceData:
    score: float = 0.0
    description: str = ""


@dataclass_json
@dataclass
class InsightData:
    id: str = ""
    headline: str = ""
    description: str = ""
    detected_at: str = ""
    signals: list[InsightSignalData] = dataclass_field(default_factory=list)
    actions: list[InsightActionData] = dataclass_field(default_factory=list)
    counterfactual: CounterfactualData = dataclass_field(default_factory=CounterfactualData)
    revenue_impact: RevenueImpactData = dataclass_field(default_factory=RevenueImpactData)
    confidence: ConfidenceData = dataclass_field(default_factory=ConfidenceData)


def _insight_to_data(insight: Insight) -> InsightData:
    return InsightData(
        id=insight.id,
        headline=insight.headline,
        description=insight.description,
        detected_at=insight.detected_at.isoformat(),
        signals=[
            InsightSignalData(
                metric_id=s.metric_id, description=s.description,
                value=s.value, change=s.change, period_days=s.period_days,
            ) for s in insight.signals
        ],
        actions=[
            InsightActionData(
                description=a.description, priority=a.priority.value,
            ) for a in insight.actions
        ],
        counterfactual=CounterfactualData(
            value=insight.counterfactual.value,
            metric_id=insight.counterfactual.metric_id,
            metric_name=insight.counterfactual.metric_name,
            description=insight.counterfactual.description,
            revenue_impact=RevenueImpactData(
                value=insight.counterfactual.revenue_impact.value,
                description=insight.counterfactual.revenue_impact.description,
            ),
        ),
        revenue_impact=RevenueImpactData(
            value=insight.revenue_impact.value,
            description=insight.revenue_impact.description,
        ),
        confidence=ConfidenceData(
            score=insight.confidence.score,
            description=insight.confidence.description,
        ),
    )


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
    data_source_store: PostgresDataSourceStore = Depends(get_data_source_store),
    metric_store: AbstractMetricStore = Depends(get_metric_store),
) -> DashboardMetric:
    if not body.metric_id:
        raise HTTPException(
            status_code=400, detail="metric_id is required",
        )
    if body.metric_id.startswith("builtin:"):
        if registry.get(body.metric_id) is None:
            raise HTTPException(status_code=404, detail="Metric not found")
    try:
        agg = Aggregation(body.aggregation)
    except ValueError:
        agg = Aggregation.SUM
    try:
        dm = dashboard_store.add_metric(project_id, body.metric_id, agg)
    except Exception as e:
        if "unique" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(
                status_code=409, detail="Metric already on dashboard",
            )
        raise

    try:
        from kpidebug.metrics.context import MetricContext
        from kpidebug.metrics.expression_metric import ExpressionMetric
        from kpidebug.metrics.types import MetricSnapshot

        metric = registry.get(body.metric_id)
        if metric is None:
            defn = metric_store.get_definition(project_id, body.metric_id)
            if defn:
                metric = ExpressionMetric(defn)
        if metric:
            ctx = MetricContext.for_project(project_id, data_source_store)
            series = metric.compute_series(ctx, aggregation=agg, days=60)
            snapshot = MetricSnapshot(metric_id=body.metric_id, project_id=project_id, values=series.values)
            dashboard_store.store_snapshot(project_id, body.metric_id, snapshot)
    except Exception:
        pass

    return dm


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
        source_type = metric.source_type.value if metric else "custom"

        if snapshot is None:
            results.append(DashboardMetricData(
                dashboard_metric_id=dm.id,
                metric_id=dm.metric_id,
                source_type=source_type,
                metric_key=metric_key,
                name=name,
                description=description,
                data_type=data_type,
                aggregation=dm.aggregation.value,
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
            source_type=source_type,
            metric_key=metric_key,
            name=name,
            description=description,
            data_type=data_type,
            aggregation=dm.aggregation.value,
            current_value=snapshot.value,
            value_1d=snapshot.aggregate_value(1, dm.aggregation),
            value_3d=snapshot.aggregate_value(3, dm.aggregation),
            value_7d=snapshot.aggregate_value(7, dm.aggregation),
            value_30d=snapshot.aggregate_value(30, dm.aggregation),
            sparkline=sparkline,
            change_1d=snapshot.change(1, dm.aggregation),
            change_3d=snapshot.change(3, dm.aggregation),
            change_7d=snapshot.change(7, dm.aggregation),
            change_30d=snapshot.change(30, dm.aggregation),
        ))

    return DashboardComputeResponse(metrics=results)


@router.get("/insights")
def list_dashboard_insights(
    project_id: str,
    limit: int = 20,
    _member: ProjectMember = Depends(require_project_role(Role.READ)),
    insight_store: AbstractInsightStore = Depends(get_insight_store),
) -> list[InsightData]:
    insights = insight_store.list_insights(project_id, limit=limit)
    return [_insight_to_data(i) for i in insights]
