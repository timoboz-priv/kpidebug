from __future__ import annotations

import logging

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.analyzer_template import InsightTemplate
from kpidebug.analysis.types import (
    Action, Insight, Priority, Signal, UpsidePotential,
)
from kpidebug.analysis.utils import (
    NEGLIGIBLE_THRESHOLD,
    TREND_WINDOW_DAYS,
    ChangeCategory,
    classify_change,
)
from kpidebug.data.types import Aggregation
from kpidebug.metrics.types import MetricSnapshot

logger = logging.getLogger(__name__)

SESSIONS_METRIC_ID = "builtin:ga.sessions"
CONVERSION_RATE_METRIC_ID = "builtin:ga.conversion_rate"

CHANNEL_DROP_THRESHOLD = -0.30

DESCRIPTION = (
    "Detects when website traffic drops significantly while "
    "conversion rate remains stable, indicating the problem is "
    "in acquisition (fewer visitors) rather than in the product "
    "or funnel. Drills into traffic channels to find the source."
)


class AcquisitionDropTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        sessions_dm = ctx.get_dashboard_metric(SESSIONS_METRIC_ID)
        conversion_dm = ctx.get_dashboard_metric(
            CONVERSION_RATE_METRIC_ID,
        )

        if sessions_dm is None or sessions_dm.snapshot is None:
            return None
        if conversion_dm is None or conversion_dm.snapshot is None:
            return None

        sessions_snapshot = sessions_dm.snapshot
        conversion_snapshot = conversion_dm.snapshot

        sessions_change = sessions_snapshot.change(
            TREND_WINDOW_DAYS, sessions_dm.aggregation,
        )
        conversion_change = conversion_snapshot.change(
            TREND_WINDOW_DAYS, conversion_dm.aggregation,
        )

        sessions_category = classify_change(sessions_change)
        is_significant_drop = sessions_category in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        )
        conversion_stable = (
            abs(conversion_change) < NEGLIGIBLE_THRESHOLD
        )

        if not is_significant_drop or not conversion_stable:
            return None

        signals = [
            Signal(
                metric_id=SESSIONS_METRIC_ID,
                description=(
                    f"Sessions dropped "
                    f"{abs(sessions_change) * 100:.1f}% "
                    f"over {TREND_WINDOW_DAYS}d"
                ),
                value=sessions_snapshot.value,
                change=sessions_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=CONVERSION_RATE_METRIC_ID,
                description=(
                    f"Conversion rate stable "
                    f"({conversion_change * 100:+.1f}%)"
                ),
                value=conversion_snapshot.value,
                change=conversion_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        actions = [
            Action(
                description=(
                    "Check acquisition channels for "
                    "source of traffic decline"
                ),
                priority=Priority.HIGH,
            ),
        ]

        upside = self._estimate_upside(
            sessions_snapshot, sessions_dm.aggregation,
        )

        channel_signal = self._check_channel_breakdown(ctx)
        if channel_signal is not None:
            signals.append(channel_signal)
            actions.insert(0, Action(
                description=(
                    f"Investigate {channel_signal.description}"
                ),
                priority=Priority.HIGH,
            ))

        return Insight(
            headline=(
                "Traffic decline while conversion holds "
                "— likely an acquisition problem"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            upside_potential=upside,
        )

    def _check_channel_breakdown(
        self, ctx: AnalysisContext,
    ) -> Signal | None:
        sessions_metric = ctx.get_metric(SESSIONS_METRIC_ID)
        if (
            sessions_metric is None
            or not sessions_metric.dimensions
        ):
            return None

        channel_dim = None
        for dim in sessions_metric.dimensions:
            if dim.key == "session_channel_group":
                channel_dim = dim
                break
        if channel_dim is None:
            return None

        try:
            series = sessions_metric.compute_series(
                ctx._metric_context,
                dimensions=[channel_dim.key],
                aggregation=Aggregation.SUM,
                days=TREND_WINDOW_DAYS * 2,
            )
        except Exception:
            logger.debug(
                "Could not compute channel breakdown "
                "for acquisition drop template",
            )
            return None

        channel_recent: dict[str, float] = {}
        channel_previous: dict[str, float] = {}
        midpoint = len(series.points) // 2

        for i, point in enumerate(series.points):
            for result in point.results:
                channel = (
                    next(iter(result.groups.values()), "unknown")
                    if result.groups else "unknown"
                )
                bucket = (
                    channel_recent
                    if i >= midpoint
                    else channel_previous
                )
                bucket[channel] = (
                    bucket.get(channel, 0.0) + result.value
                )

        worst_channel = ""
        worst_change = 0.0
        for channel, recent in channel_recent.items():
            previous = channel_previous.get(channel, 0.0)
            if previous == 0:
                continue
            change = (recent - previous) / abs(previous)
            if change < worst_change:
                worst_change = change
                worst_channel = channel

        if worst_change <= CHANNEL_DROP_THRESHOLD and worst_channel:
            return Signal(
                metric_id=SESSIONS_METRIC_ID,
                description=(
                    f"Channel '{worst_channel}' dropped "
                    f"{abs(worst_change) * 100:.0f}%"
                ),
                value=channel_recent.get(worst_channel, 0.0),
                change=worst_change,
                period_days=TREND_WINDOW_DAYS,
            )

        return None

    def _estimate_upside(
        self,
        sessions_snapshot: MetricSnapshot,
        aggregation: Aggregation,
    ) -> UpsidePotential:
        previous = (
            sessions_snapshot.aggregate_value(
                TREND_WINDOW_DAYS * 2, aggregation,
            )
            - sessions_snapshot.aggregate_value(
                TREND_WINDOW_DAYS, aggregation,
            )
        )
        recent = sessions_snapshot.aggregate_value(
            TREND_WINDOW_DAYS, aggregation,
        )
        lost = max(previous - recent, 0.0)
        return UpsidePotential(
            value=lost,
            metric_id=SESSIONS_METRIC_ID,
            metric_name="Sessions",
            description=(
                f"~{lost:.0f} sessions recoverable by "
                f"restoring traffic to prior levels"
            ),
        )
