from __future__ import annotations

import logging

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.analyzer_template import InsightTemplate
from kpidebug.analysis.types import (
    Action, Confidence, Counterfactual, Insight, Priority,
    RevenueImpact, Signal,
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
GROSS_REVENUE_METRIC_ID = "builtin:stripe.gross_revenue"

CHANNEL_DROP_THRESHOLD = -0.30

DESCRIPTION = (
    "Fewer people are visiting your site, but those who do "
    "convert at the same rate as before. That means your product "
    "and pricing are fine — the issue is that fewer potential "
    "customers are finding you. We looked at your traffic "
    "channels to pinpoint where the drop is coming from."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


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
                    f"Sessions ↓ {abs(sessions_change) * 100:.1f}%"
                ),
                value=sessions_snapshot.value,
                change=sessions_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=CONVERSION_RATE_METRIC_ID,
                description=(
                    f"Conversion rate ~ "
                    f"{conversion_snapshot.value:.1f}% "
                    f"(stable)"
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

        channel_signal = self._check_channel_breakdown(ctx)
        has_channel = channel_signal is not None
        if has_channel:
            signals.append(channel_signal)
            actions.insert(0, Action(
                description=(
                    f"Investigate {channel_signal.description}"
                ),
                priority=Priority.HIGH,
            ))

        revenue_impact = self._estimate_revenue_impact(ctx)
        counterfactual = self._estimate_counterfactual(
            sessions_snapshot, sessions_dm.aggregation,
            conversion_snapshot, ctx, revenue_impact,
        )
        confidence = self._compute_confidence(
            conversion_change, sessions_category, has_channel,
        )

        return Insight(
            headline=(
                "Traffic decline while conversion holds "
                "— likely an acquisition problem"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            counterfactual=counterfactual,
            revenue_impact=revenue_impact,
            confidence=confidence,
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
                    f"{worst_channel} ↓ "
                    f"{abs(worst_change) * 100:.0f}%"
                ),
                value=channel_recent.get(worst_channel, 0.0),
                change=worst_change,
                period_days=TREND_WINDOW_DAYS,
            )

        return None

    def _estimate_revenue_impact(
        self,
        ctx: AnalysisContext,
    ) -> RevenueImpact:
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_METRIC_ID)
        if rev_dm is None or rev_dm.snapshot is None:
            return RevenueImpact()

        rev_change = rev_dm.snapshot.change(
            TREND_WINDOW_DAYS, rev_dm.aggregation,
        )
        if rev_change >= 0:
            return RevenueImpact()

        recent_rev = rev_dm.snapshot.aggregate_value(
            TREND_WINDOW_DAYS, rev_dm.aggregation,
        )
        previous_rev = (
            rev_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS * 2, rev_dm.aggregation,
            )
            - recent_rev
        )
        lost_rev = max(previous_rev - recent_rev, 0.0) / 100
        return RevenueImpact(
            value=lost_rev,
            description=f"Impact: -{_fmt_dollars(lost_rev)} (7d)",
        )

    def _estimate_counterfactual(
        self,
        sessions_snapshot: MetricSnapshot,
        aggregation: Aggregation,
        conversion_snapshot: MetricSnapshot,
        ctx: AnalysisContext,
        revenue_impact: RevenueImpact,
    ) -> Counterfactual:
        previous_sessions = (
            sessions_snapshot.aggregate_value(
                TREND_WINDOW_DAYS * 2, aggregation,
            )
            - sessions_snapshot.aggregate_value(
                TREND_WINDOW_DAYS, aggregation,
            )
        )
        recent_sessions = sessions_snapshot.aggregate_value(
            TREND_WINDOW_DAYS, aggregation,
        )
        lost_sessions = max(previous_sessions - recent_sessions, 0.0)
        if lost_sessions <= 0:
            return Counterfactual()

        conv_rate = conversion_snapshot.value / 100.0
        lost_conversions = lost_sessions * conv_rate

        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_METRIC_ID)
        rev_recovery = RevenueImpact()
        if rev_dm and rev_dm.snapshot and recent_sessions > 0:
            recent_rev = rev_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, rev_dm.aggregation,
            )
            rev_per_session = recent_rev / recent_sessions / 100
            recoverable = lost_sessions * rev_per_session
            recoverable = min(recoverable, revenue_impact.value) if revenue_impact.value > 0 else recoverable
            rev_recovery = RevenueImpact(
                value=recoverable,
                description=(
                    f"Recovery: ~{_fmt_dollars(recoverable)} (7d)"
                ),
            )

        return Counterfactual(
            value=lost_conversions,
            metric_id=SESSIONS_METRIC_ID,
            metric_name="Sessions",
            description=(
                f"~{lost_sessions:.0f} sessions / "
                f"~{lost_conversions:.0f} conversions "
                f"recoverable"
            ),
            revenue_impact=rev_recovery,
        )

    def _compute_confidence(
        self,
        conversion_change: float,
        sessions_category: ChangeCategory,
        has_channel_breakdown: bool,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        if sessions_category == ChangeCategory.LARGE_DROP:
            score += 0.15
            reasons.append("large traffic drop")
        else:
            score += 0.05
            reasons.append("moderate traffic drop")

        conv_stability = 1.0 - abs(conversion_change) / NEGLIGIBLE_THRESHOLD
        score += 0.15 * max(conv_stability, 0.0)
        if conv_stability > 0.5:
            reasons.append("conversion very stable")
        else:
            reasons.append("conversion mostly stable")

        if has_channel_breakdown:
            score += 0.15
            reasons.append("specific channel identified")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
