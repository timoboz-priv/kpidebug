import logging

from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.metrics.context import MetricContext
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.expression_metric import ExpressionMetric
from kpidebug.metrics.types import Metric
from kpidebug.metrics.metric_store import AbstractMetricStore
from kpidebug.metrics.types import MetricSnapshot
import kpidebug.metrics.registry as registry

logger = logging.getLogger(__name__)


def process_all(
    project_id: str,
    data_source_store: PostgresDataSourceStore,
    dashboard_store: AbstractDashboardStore,
    metric_store: AbstractMetricStore,
) -> None:
    from kpidebug.api.routes_data_sources import make_connector

    sources = data_source_store.list_sources(project_id)
    for source in sources:
        try:
            connector = make_connector(source, data_source_store)
            connector.sync_all()
            logger.info("Synced data source %s", source.name)
        except Exception as e:
            logger.warning("Failed to sync source %s: %s", source.name, e)

    ctx = MetricContext.for_project(project_id, data_source_store)

    pinned = dashboard_store.list_metrics(project_id)
    if not pinned:
        return

    for dm in pinned:
        try:
            metric = _resolve_metric(dm.metric_id, project_id, metric_store)
            if metric is None:
                continue

            series = metric.compute_series(ctx, days=30)
            values = series.values
            snapshot = MetricSnapshot(
                metric_id=dm.metric_id,
                project_id=project_id,
                values=values,
            )
            dashboard_store.store_snapshot(project_id, dm.metric_id, snapshot)
            logger.info("Computed snapshot for metric %s", dm.metric_id)
        except Exception as e:
            logger.warning("Failed to process metric %s: %s", dm.metric_id, e)


def _resolve_metric(metric_id: str, project_id: str, metric_store: AbstractMetricStore) -> Metric | None:
    builtin = registry.get(metric_id)
    if builtin is not None:
        return builtin

    definition = metric_store.get_definition(project_id, metric_id)
    if definition is not None:
        return ExpressionMetric(definition)

    return None
