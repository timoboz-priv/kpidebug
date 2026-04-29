from __future__ import annotations

import logging

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.analyzer_template import InsightTemplate
from kpidebug.analysis.types import (
    Action, Confidence, Counterfactual, Insight, Priority,
    RevenueImpact, Signal,
)
from kpidebug.analysis.utils import (
    TREND_WINDOW_DAYS,
    ChangeCategory,
    classify_change,
)
from kpidebug.data.types import Aggregation

logger = logging.getLogger(__name__)

SESSIONS_METRIC_ID = "builtin:ga.sessions"
SESSIONS_BY_COUNTRY_ID = "builtin:ga.sessions_by_country"
GROSS_REVENUE_METRIC_ID = "builtin:stripe.gross_revenue"

GLOBAL_DROP_THRESHOLD = -0.10
SEGMENT_DROP_THRESHOLD = -0.25
SEGMENT_MULTIPLIER = 2.0

DESCRIPTION = (
    "Your overall numbers are down, but the drop is "
    "concentrated in one specific market or region. "
    "The rest of your business is performing normally — "
    "this is a localized problem worth investigating."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


class SegmentFailureTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        sessions_dm = ctx.get_dashboard_metric(SESSIONS_METRIC_ID)
        if sessions_dm is None or sessions_dm.snapshot is None:
            return None

        global_change = sessions_dm.snapshot.change(
            TREND_WINDOW_DAYS, sessions_dm.aggregation,
        )
        if global_change >= GLOBAL_DROP_THRESHOLD:
            return None

        segment = self._find_worst_segment(ctx)
        if segment is None:
            return None

        seg_name, seg_change, seg_recent, seg_previous = segment

        if seg_change >= SEGMENT_DROP_THRESHOLD:
            return None
        if abs(seg_change) < abs(global_change) * SEGMENT_MULTIPLIER:
            return None

        signals = [
            Signal(
                metric_id=SESSIONS_METRIC_ID,
                description=(
                    f"Overall sessions ↓ "
                    f"{abs(global_change) * 100:.1f}%"
                ),
                value=sessions_dm.snapshot.value,
                change=global_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=SESSIONS_BY_COUNTRY_ID,
                description=(
                    f"{seg_name} ↓ "
                    f"{abs(seg_change) * 100:.0f}%"
                ),
                value=seg_recent,
                change=seg_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        actions = [
            Action(
                description=(
                    f"Investigate what changed in {seg_name} "
                    f"— ad spend, regulations, seasonality, "
                    f"or localization issues"
                ),
                priority=Priority.HIGH,
            ),
        ]

        revenue_impact = self._estimate_revenue_impact(ctx)
        lost_sessions = max(seg_previous - seg_recent, 0.0)
        seg_rev_recovery = self._estimate_segment_recovery(
            ctx, seg_name, revenue_impact,
        )
        counterfactual = Counterfactual(
            value=lost_sessions,
            metric_id=SESSIONS_BY_COUNTRY_ID,
            metric_name="Sessions",
            description=(
                f"~{lost_sessions:.0f} sessions recoverable "
                f"in {seg_name}"
            ),
            revenue_impact=seg_rev_recovery,
        )
        confidence = self._compute_confidence(
            global_change, seg_change,
        )

        return Insight(
            headline=(
                f"Traffic collapse in {seg_name} is dragging "
                f"down overall numbers"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            counterfactual=counterfactual,
            revenue_impact=revenue_impact,
            confidence=confidence,
        )

    def _find_worst_segment(
        self, ctx: AnalysisContext,
    ) -> tuple[str, float, float, float] | None:
        metric = ctx.get_metric(SESSIONS_BY_COUNTRY_ID)
        if metric is None or not metric.dimensions:
            return None

        try:
            series = metric.compute_series(
                ctx._metric_context,
                dimensions=["country"],
                aggregation=Aggregation.SUM,
                days=TREND_WINDOW_DAYS * 2,
                date=ctx.as_of_date,
            )
        except Exception:
            return None

        segment_recent: dict[str, float] = {}
        segment_previous: dict[str, float] = {}
        midpoint = len(series.points) // 2

        for i, point in enumerate(series.points):
            for result in point.results:
                name = (
                    next(iter(result.groups.values()), "unknown")
                    if result.groups else "unknown"
                )
                bucket = (
                    segment_recent if i >= midpoint
                    else segment_previous
                )
                bucket[name] = (
                    bucket.get(name, 0.0) + result.value
                )

        worst_name = ""
        worst_change = 0.0
        worst_recent = 0.0
        worst_previous = 0.0
        for name, recent in segment_recent.items():
            previous = segment_previous.get(name, 0.0)
            if previous < 50:
                continue
            change = (recent - previous) / abs(previous)
            if change < worst_change:
                worst_change = change
                worst_name = name
                worst_recent = recent
                worst_previous = previous

        if not worst_name:
            return None
        return worst_name, worst_change, worst_recent, worst_previous

    def _estimate_revenue_impact(
        self, ctx: AnalysisContext,
    ) -> RevenueImpact:
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_METRIC_ID)
        if rev_dm is None or rev_dm.snapshot is None:
            return RevenueImpact()
        rev_change = rev_dm.snapshot.change(
            TREND_WINDOW_DAYS, rev_dm.aggregation,
        )
        if rev_change >= 0:
            return RevenueImpact()
        recent = rev_dm.snapshot.aggregate_value(
            TREND_WINDOW_DAYS, rev_dm.aggregation,
        )
        previous = (
            rev_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS * 2, rev_dm.aggregation,
            ) - recent
        )
        lost = max(previous - recent, 0.0) / 100
        return RevenueImpact(
            value=lost,
            description=f"Impact: -{_fmt_dollars(lost)} (7d)",
        )

    def _estimate_segment_recovery(
        self,
        ctx: AnalysisContext,
        segment_name: str,
        revenue_impact: RevenueImpact,
    ) -> RevenueImpact:
        metric = ctx.get_metric(GROSS_REVENUE_METRIC_ID)
        if metric is None:
            return RevenueImpact()

        try:
            series = metric.compute_series(
                ctx._metric_context,
                dimensions=["country"],
                aggregation=Aggregation.SUM,
                days=TREND_WINDOW_DAYS * 2,
                date=ctx.as_of_date,
            )
        except Exception:
            return RevenueImpact()

        seg_recent = 0.0
        seg_previous = 0.0
        midpoint = len(series.points) // 2
        for i, point in enumerate(series.points):
            for result in point.results:
                country = (
                    next(iter(result.groups.values()), "")
                    if result.groups else ""
                )
                if country != segment_name:
                    continue
                if i >= midpoint:
                    seg_recent += result.value
                else:
                    seg_previous += result.value

        lost = max(seg_previous - seg_recent, 0.0) / 100
        if lost <= 0:
            return RevenueImpact()
        if revenue_impact.value > 0:
            lost = min(lost, revenue_impact.value)
        return RevenueImpact(
            value=lost,
            description=f"Recovery: ~{_fmt_dollars(lost)} (7d)",
        )

    def _compute_confidence(
        self,
        global_change: float,
        segment_change: float,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        ratio = abs(segment_change) / abs(global_change) if global_change != 0 else 0
        if ratio >= 3.0:
            score += 0.15
            reasons.append("segment drop 3x+ global")
        else:
            score += 0.05
            reasons.append("segment drop 2x+ global")

        if abs(segment_change) >= 0.40:
            score += 0.10
            reasons.append("severe segment decline")

        if abs(global_change) >= 0.15:
            score += 0.10
            reasons.append("material global impact")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
