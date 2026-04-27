import logging
import time

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

    total_start = time.monotonic()
    logger.info("Starting process_all for project %s", project_id)

    sources = data_source_store.list_sources(project_id)
    logger.info("Found %d data source(s) to sync", len(sources))

    for source in sources:
        sync_start = time.monotonic()
        try:
            connector = make_connector(source, data_source_store)
            result = connector.sync_all()
            elapsed = time.monotonic() - sync_start
            table_summary = ", ".join(f"{k}: {v} rows" for k, v in result.tables.items())
            logger.info("Synced source '%s' in %.1fs — %s", source.name, elapsed, table_summary)
            if result.errors:
                for err in result.errors:
                    logger.warning("  Sync error in table '%s': %s", err.table, err.error)
        except Exception as e:
            elapsed = time.monotonic() - sync_start
            logger.error("Failed to sync source '%s' after %.1fs: %s", source.name, elapsed, e)

    logger.info("Building metric context")
    ctx = MetricContext.for_project(project_id, data_source_store)

    pinned = dashboard_store.list_metrics(project_id)
    if not pinned:
        logger.info("No dashboard metrics pinned, skipping computation")
        return

    logger.info("Computing snapshots for %d pinned metric(s)", len(pinned))
    computed = 0
    failed = 0

    for dm in pinned:
        metric_start = time.monotonic()
        try:
            metric = _resolve_metric(dm.metric_id, project_id, metric_store)
            if metric is None:
                logger.warning("Could not resolve metric %s, skipping", dm.metric_id)
                failed += 1
                continue

            logger.debug("Computing series for metric '%s' (%s)", metric.name, dm.metric_id)
            series = metric.compute_series(ctx, days=60)
            values = series.values

            snapshot = MetricSnapshot(
                metric_id=dm.metric_id,
                project_id=project_id,
                values=values,
            )
            dashboard_store.store_snapshot(project_id, dm.metric_id, snapshot)

            elapsed = time.monotonic() - metric_start
            latest = snapshot.value
            logger.info("Computed metric '%s' in %.1fs — latest value: %.2f, %d data points",
                        metric.name, elapsed, latest, len(values))
            computed += 1
        except Exception as e:
            elapsed = time.monotonic() - metric_start
            logger.error("Failed metric '%s' after %.1fs: %s", dm.metric_id, elapsed, e)
            failed += 1

    total_elapsed = time.monotonic() - total_start
    logger.info("process_all complete in %.1fs — %d computed, %d failed",
                total_elapsed, computed, failed)


def _resolve_metric(metric_id: str, project_id: str, metric_store: AbstractMetricStore) -> Metric | None:
    builtin = registry.get(metric_id)
    if builtin is not None:
        return builtin

    definition = metric_store.get_definition(project_id, metric_id)
    if definition is not None:
        return ExpressionMetric(definition)

    return None
