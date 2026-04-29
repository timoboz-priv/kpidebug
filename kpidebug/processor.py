from __future__ import annotations

import logging
import time
from datetime import date
from enum import Enum

from kpidebug.analysis.analyzer_template import TemplateAnalyzer
from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.templates.acquisition_drop import AcquisitionDropTemplate
from kpidebug.analysis.templates.cohort_quality import CohortQualityTemplate
from kpidebug.analysis.templates.conversion_breakdown import ConversionBreakdownTemplate
from kpidebug.analysis.templates.involuntary_churn import InvoluntaryChurnTemplate
from kpidebug.analysis.templates.metric_illusion import MetricIllusionTemplate
from kpidebug.analysis.templates.onboarding_failure import OnboardingFailureTemplate
from kpidebug.analysis.templates.pricing_mismatch import PricingMismatchTemplate
from kpidebug.analysis.templates.product_friction import ProductFrictionTemplate
from kpidebug.analysis.templates.returning_user_drop import ReturningUserDropTemplate
from kpidebug.analysis.templates.segment_failure import SegmentFailureTemplate
from kpidebug.analysis.types import AnalysisResult
from kpidebug.data.types import Aggregation
from kpidebug.data.data_source_store_postgres import (
    PostgresDataSourceStore,
)
from kpidebug.metrics.context import MetricContext
from kpidebug.metrics.dashboard_store import AbstractDashboardStore
from kpidebug.metrics.expression_metric import ExpressionMetric
from kpidebug.metrics.types import (
    DashboardMetric,
    Metric,
    MetricSnapshot,
)
from kpidebug.metrics.metric_store import AbstractMetricStore
import kpidebug.metrics.registry as registry

logger = logging.getLogger(__name__)

_DEFAULT_TEMPLATES = [
    AcquisitionDropTemplate(),
    ConversionBreakdownTemplate(),
    SegmentFailureTemplate(),
    ReturningUserDropTemplate(),
    InvoluntaryChurnTemplate(),
    OnboardingFailureTemplate(),
    PricingMismatchTemplate(),
    CohortQualityTemplate(),
    ProductFrictionTemplate(),
    MetricIllusionTemplate(),
]


class ProcessMode(str, Enum):
    FULL = "full"
    METRICS = "metrics"
    ANALYSIS = "analysis"


def process_all(
    project_id: str,
    data_source_store: PostgresDataSourceStore,
    dashboard_store: AbstractDashboardStore,
    metric_store: AbstractMetricStore,
    mode: ProcessMode = ProcessMode.FULL,
) -> None:
    total_start = time.monotonic()
    logger.info(
        "Starting process_all for project %s (mode=%s)",
        project_id, mode.value,
    )

    sync_elapsed = 0.0
    compute_elapsed = 0.0

    # --- Data source sync ---
    if mode == ProcessMode.FULL:
        sync_elapsed = _sync_data_sources(
            project_id, data_source_store,
        )

    # --- Metrics computation ---
    if mode in (ProcessMode.FULL, ProcessMode.METRICS):
        compute_elapsed = _compute_and_store_metrics(
            project_id, data_source_store,
            dashboard_store, metric_store,
        )

    # --- Analysis ---
    analysis_start = time.monotonic()
    ctx = MetricContext.for_project(
        project_id, data_source_store,
    )
    refreshed = dashboard_store.list_metrics(project_id)
    analysis_result = _run_analysis(
        project_id, refreshed, ctx,
        metric_store, data_source_store,
    )
    analysis_elapsed = time.monotonic() - analysis_start
    _log_analysis_result(analysis_result, analysis_elapsed)

    total_elapsed = time.monotonic() - total_start
    logger.info(
        "process_all complete in %.1fs "
        "(sync: %.1fs, compute: %.1fs, analysis: %.1fs)",
        total_elapsed, sync_elapsed,
        compute_elapsed, analysis_elapsed,
    )


def process_simulate(
    project_id: str,
    data_source_store: PostgresDataSourceStore,
    dashboard_store: AbstractDashboardStore,
    metric_store: AbstractMetricStore,
    as_of_date: date | None = None,
) -> AnalysisResult:
    total_start = time.monotonic()
    date_label = as_of_date.isoformat() if as_of_date else "today"
    logger.info(
        "Starting process_simulate for project %s "
        "(as_of_date=%s)",
        project_id, date_label,
    )

    ctx = MetricContext.for_project(
        project_id, data_source_store,
    )
    pinned = dashboard_store.list_metrics(project_id)

    if not pinned:
        logger.info("No dashboard metrics pinned, nothing to simulate")
        return AnalysisResult()

    # --- Compute metrics into in-memory snapshots ---
    compute_start = time.monotonic()
    simulated_metrics = _compute_simulated_metrics(
        pinned, project_id, ctx, metric_store, as_of_date,
    )
    compute_elapsed = time.monotonic() - compute_start
    logger.info(
        "Simulated %d metric(s) in %.1fs",
        len(simulated_metrics), compute_elapsed,
    )

    # --- Analysis ---
    analysis_start = time.monotonic()
    analysis_result = _run_analysis(
        project_id, simulated_metrics, ctx,
        metric_store, data_source_store,
        as_of_date=as_of_date,
    )
    analysis_elapsed = time.monotonic() - analysis_start
    _log_analysis_result(analysis_result, analysis_elapsed)

    total_elapsed = time.monotonic() - total_start
    logger.info(
        "process_simulate complete in %.1fs "
        "(compute: %.1fs, analysis: %.1fs)",
        total_elapsed, compute_elapsed, analysis_elapsed,
    )
    return analysis_result


# --- Internal helpers ---


def _sync_data_sources(
    project_id: str,
    data_source_store: PostgresDataSourceStore,
) -> float:
    from kpidebug.api.routes_data_sources import make_connector

    sync_start = time.monotonic()
    sources = data_source_store.list_sources(project_id)
    logger.info(
        "Found %d data source(s) to sync", len(sources),
    )

    for source in sources:
        source_start = time.monotonic()
        try:
            connector = make_connector(
                source, data_source_store,
            )
            result = connector.sync_all()
            elapsed = time.monotonic() - source_start
            table_summary = ", ".join(
                f"{k}: {v} rows"
                for k, v in result.tables.items()
            )
            logger.info(
                "Synced source '%s' in %.1fs — %s",
                source.name, elapsed, table_summary,
            )
            if result.errors:
                for err in result.errors:
                    logger.warning(
                        "  Sync error in table '%s': %s",
                        err.table, err.error,
                    )
        except Exception as e:
            elapsed = time.monotonic() - source_start
            logger.error(
                "Failed to sync source '%s' after %.1fs: %s",
                source.name, elapsed, e,
            )

    sync_elapsed = time.monotonic() - sync_start
    logger.info(
        "Data source sync complete in %.1fs", sync_elapsed,
    )
    return sync_elapsed


def _compute_and_store_metrics(
    project_id: str,
    data_source_store: PostgresDataSourceStore,
    dashboard_store: AbstractDashboardStore,
    metric_store: AbstractMetricStore,
) -> float:
    compute_start = time.monotonic()
    logger.info("Building metric context")
    ctx = MetricContext.for_project(
        project_id, data_source_store,
    )

    pinned = dashboard_store.list_metrics(project_id)
    if not pinned:
        logger.info(
            "No dashboard metrics pinned, skipping computation",
        )
        return time.monotonic() - compute_start

    logger.info(
        "Computing snapshots for %d pinned metric(s)",
        len(pinned),
    )
    computed = 0
    failed = 0

    for dm in pinned:
        metric_start = time.monotonic()
        try:
            metric = _resolve_metric(
                dm.metric_id, project_id, metric_store,
            )
            if metric is None:
                logger.warning(
                    "Could not resolve metric %s, skipping",
                    dm.metric_id,
                )
                failed += 1
                continue

            series = metric.compute_series(
                ctx, aggregation=dm.aggregation, days=60,
            )
            snapshot = MetricSnapshot(
                metric_id=dm.metric_id,
                project_id=project_id,
                values=series.values,
            )
            dashboard_store.store_snapshot(
                project_id, dm.metric_id, snapshot,
            )

            elapsed = time.monotonic() - metric_start
            logger.info(
                "Computed %-25s in %.1fs — %s",
                metric.name, elapsed,
                _format_snapshot_summary(
                    snapshot, dm.aggregation,
                ),
            )
            computed += 1
        except Exception as e:
            elapsed = time.monotonic() - metric_start
            logger.error(
                "Failed metric '%s' after %.1fs: %s",
                dm.metric_id, elapsed, e,
            )
            failed += 1

    compute_elapsed = time.monotonic() - compute_start
    logger.info(
        "Metrics computation complete in %.1fs "
        "— %d computed, %d failed",
        compute_elapsed, computed, failed,
    )
    return compute_elapsed


def _compute_simulated_metrics(
    pinned: list[DashboardMetric],
    project_id: str,
    ctx: MetricContext,
    metric_store: AbstractMetricStore,
    as_of_date: date | None,
) -> list[DashboardMetric]:
    simulated: list[DashboardMetric] = []

    for dm in pinned:
        metric = _resolve_metric(
            dm.metric_id, project_id, metric_store,
        )
        if metric is None:
            logger.warning(
                "Could not resolve metric %s, skipping",
                dm.metric_id,
            )
            continue

        try:
            series = metric.compute_series(
                ctx, aggregation=dm.aggregation,
                days=60, date=as_of_date,
            )
            snapshot = MetricSnapshot(
                metric_id=dm.metric_id,
                project_id=project_id,
                values=series.values,
            )
            sim_dm = DashboardMetric(
                id=dm.id,
                project_id=dm.project_id,
                metric_id=dm.metric_id,
                aggregation=dm.aggregation,
                position=dm.position,
                added_at=dm.added_at,
                snapshot=snapshot,
            )
            simulated.append(sim_dm)
            logger.info(
                "Simulated %-25s — %s",
                metric.name,
                _format_snapshot_summary(
                    snapshot, dm.aggregation,
                ),
            )
        except Exception as e:
            logger.error(
                "Failed to simulate metric '%s': %s",
                dm.metric_id, e,
            )

    return simulated


def _run_analysis(
    project_id: str,
    dashboard_metrics: list[DashboardMetric],
    metric_context: MetricContext,
    metric_store: AbstractMetricStore,
    data_source_store: PostgresDataSourceStore,
    as_of_date: date | None = None,
) -> AnalysisResult:
    analysis_ctx = AnalysisContext(
        project_id=project_id,
        dashboard_metrics=dashboard_metrics,
        metric_context=metric_context,
        metric_store=metric_store,
        data_source_store=data_source_store,
        as_of_date=as_of_date,
    )

    analyzer = TemplateAnalyzer(_DEFAULT_TEMPLATES)
    return analyzer.analyze(analysis_ctx)


def _log_analysis_result(
    result: AnalysisResult,
    elapsed: float,
) -> None:
    logger.info(
        "Analysis complete in %.1fs — %d insight(s) found",
        elapsed, len(result.insights),
    )
    for insight in result.insights:
        logger.info(
            "  Insight: %s (%d signals, %d actions)",
            insight.headline,
            len(insight.signals),
            len(insight.actions),
        )


def _format_snapshot_summary(
    snapshot: MetricSnapshot,
    aggregation: Aggregation,
) -> str:
    v1 = snapshot.aggregate_value(1, aggregation)
    v3 = snapshot.aggregate_value(3, aggregation)
    v7 = snapshot.aggregate_value(7, aggregation)
    v30 = snapshot.aggregate_value(30, aggregation)
    c1 = snapshot.change(1, aggregation)
    c3 = snapshot.change(3, aggregation)
    c7 = snapshot.change(7, aggregation)
    c30 = snapshot.change(30, aggregation)
    return (
        f"1d: {v1:>12,.2f} ({c1:+6.1%})  "
        f"3d: {v3:>12,.2f} ({c3:+6.1%})  "
        f"7d: {v7:>12,.2f} ({c7:+6.1%})  "
        f"30d: {v30:>12,.2f} ({c30:+6.1%})"
    )


def _resolve_metric(
    metric_id: str,
    project_id: str,
    metric_store: AbstractMetricStore,
) -> Metric | None:
    builtin = registry.get(metric_id)
    if builtin is not None:
        return builtin

    definition = metric_store.get_definition(
        project_id, metric_id,
    )
    if definition is not None:
        return ExpressionMetric(definition)

    return None
