from __future__ import annotations

import logging

from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.analyzer_template import InsightTemplate
from kpidebug.analysis.types import (
    Action, Confidence, Counterfactual, Insight, Priority,
    RevenueImpact, Signal,
)
from kpidebug.analysis.utils import (
    COMPARISON_WINDOW_DAYS,
    TREND_WINDOW_DAYS,
    ChangeCategory,
    classify_change,
)

logger = logging.getLogger(__name__)

ENGAGEMENT_RATE_ID = "builtin:ga.engagement_rate"
SESSION_DURATION_ID = "builtin:ga.avg_session_duration"
BOUNCE_RATE_ID = "builtin:ga.bounce_rate"
SESSIONS_ID = "builtin:ga.sessions"
GROSS_REVENUE_ID = "builtin:stripe.gross_revenue"
RETENTION_RATE_ID = "builtin:stripe.retention_rate"

TREND_THRESHOLD = -0.03

DESCRIPTION = (
    "Your engagement metrics are slowly declining over weeks, "
    "not crashing overnight. This gradual erosion usually "
    "signals accumulating product friction — small UX issues, "
    "performance degradation, or feature bloat that "
    "individually seem minor but compound over time."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


class ProductFrictionTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        engagement_dm = ctx.get_dashboard_metric(ENGAGEMENT_RATE_ID)
        duration_dm = ctx.get_dashboard_metric(SESSION_DURATION_ID)

        if engagement_dm is None or engagement_dm.snapshot is None:
            return None

        engagement_30d = engagement_dm.snapshot.change(
            COMPARISON_WINDOW_DAYS, engagement_dm.aggregation,
        )
        engagement_declining = engagement_30d <= TREND_THRESHOLD

        duration_declining = False
        if duration_dm and duration_dm.snapshot:
            duration_30d = duration_dm.snapshot.change(
                COMPARISON_WINDOW_DAYS, duration_dm.aggregation,
            )
            duration_declining = duration_30d <= TREND_THRESHOLD

        if not engagement_declining and not duration_declining:
            return None

        engagement_7d = engagement_dm.snapshot.change(
            TREND_WINDOW_DAYS, engagement_dm.aggregation,
        )
        eng_cat = classify_change(engagement_7d)
        if eng_cat in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        ):
            return None

        signals = []

        if engagement_declining:
            signals.append(Signal(
                metric_id=ENGAGEMENT_RATE_ID,
                description=(
                    f"Engagement ↓ "
                    f"{abs(engagement_30d) * 100:.1f}% (30d trend)"
                ),
                value=engagement_dm.snapshot.value,
                change=engagement_30d,
                period_days=COMPARISON_WINDOW_DAYS,
            ))

        if duration_declining and duration_dm and duration_dm.snapshot:
            duration_30d = duration_dm.snapshot.change(
                COMPARISON_WINDOW_DAYS, duration_dm.aggregation,
            )
            signals.append(Signal(
                metric_id=SESSION_DURATION_ID,
                description=(
                    f"Session duration ↓ "
                    f"{abs(duration_30d) * 100:.1f}% (30d trend)"
                ),
                value=duration_dm.snapshot.value,
                change=duration_30d,
                period_days=COMPARISON_WINDOW_DAYS,
            ))

        bounce_dm = ctx.get_dashboard_metric(BOUNCE_RATE_ID)
        bounce_rising = False
        if bounce_dm and bounce_dm.snapshot:
            bounce_30d = bounce_dm.snapshot.change(
                COMPARISON_WINDOW_DAYS, bounce_dm.aggregation,
            )
            bounce_cat = classify_change(bounce_30d)
            if bounce_cat in (
                ChangeCategory.SMALL_GAIN,
                ChangeCategory.LARGE_GAIN,
            ):
                bounce_rising = True
                signals.append(Signal(
                    metric_id=BOUNCE_RATE_ID,
                    description=(
                        f"Bounce rate ↑ "
                        f"{bounce_30d * 100:.1f}% (30d trend)"
                    ),
                    value=bounce_dm.snapshot.value,
                    change=bounce_30d,
                    period_days=COMPARISON_WINDOW_DAYS,
                ))

        retention_dm = ctx.get_dashboard_metric(RETENTION_RATE_ID)
        retention_declining = False
        if retention_dm and retention_dm.snapshot:
            ret_30d = retention_dm.snapshot.change(
                COMPARISON_WINDOW_DAYS, retention_dm.aggregation,
            )
            if ret_30d <= TREND_THRESHOLD:
                retention_declining = True
                signals.append(Signal(
                    metric_id=RETENTION_RATE_ID,
                    description=(
                        f"Retention ↓ "
                        f"{abs(ret_30d) * 100:.1f}% (30d trend)"
                    ),
                    value=retention_dm.snapshot.value,
                    change=ret_30d,
                    period_days=COMPARISON_WINDOW_DAYS,
                ))

        trend_consistent = self._check_trend_consistency(
            engagement_dm.snapshot,
        )

        actions = [
            Action(
                description=(
                    "Run a UX audit — check page load times, "
                    "error rates, and user flow completion"
                ),
                priority=Priority.HIGH,
            ),
            Action(
                description=(
                    "Survey recent users about pain points "
                    "and friction in the product"
                ),
                priority=Priority.MEDIUM,
            ),
        ]

        revenue_impact = self._estimate_revenue_impact(ctx)
        counterfactual = Counterfactual(
            value=abs(engagement_30d) * 100,
            metric_id=ENGAGEMENT_RATE_ID,
            metric_name="Engagement Rate",
            description=(
                f"Engagement trending down "
                f"{abs(engagement_30d) * 100:.1f}% over 30d"
            ),
        )
        confidence = self._compute_confidence(
            engagement_declining, duration_declining,
            bounce_rising, trend_consistent,
        )

        return Insight(
            headline=(
                "Engagement slowly eroding — accumulating "
                "product friction"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            counterfactual=counterfactual,
            revenue_impact=revenue_impact,
            confidence=confidence,
        )

    def _check_trend_consistency(
        self, snapshot,
    ) -> bool:
        vals = snapshot.values
        if len(vals) < COMPARISON_WINDOW_DAYS:
            return False
        first_half = vals[-COMPARISON_WINDOW_DAYS:-COMPARISON_WINDOW_DAYS // 2]
        second_half = vals[-COMPARISON_WINDOW_DAYS // 2:]
        if not first_half or not second_half:
            return False
        avg_first = sum(first_half) / len(first_half)
        avg_second = sum(second_half) / len(second_half)
        return avg_second < avg_first

    def _estimate_revenue_impact(
        self, ctx: AnalysisContext,
    ) -> RevenueImpact:
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_ID)
        if rev_dm is None or rev_dm.snapshot is None:
            return RevenueImpact()
        rev_change = rev_dm.snapshot.change(
            COMPARISON_WINDOW_DAYS, rev_dm.aggregation,
        )
        if rev_change >= 0:
            return RevenueImpact()
        recent = rev_dm.snapshot.aggregate_value(
            COMPARISON_WINDOW_DAYS, rev_dm.aggregation,
        )
        previous = (
            rev_dm.snapshot.aggregate_value(
                COMPARISON_WINDOW_DAYS * 2, rev_dm.aggregation,
            ) - recent
        )
        lost = max(previous - recent, 0.0) / 100
        return RevenueImpact(
            value=lost,
            description=f"Impact: -{_fmt_dollars(lost)} (30d)",
        )

    def _compute_confidence(
        self,
        engagement_declining: bool,
        duration_declining: bool,
        bounce_rising: bool,
        trend_consistent: bool,
    ) -> Confidence:
        score = 0.4
        reasons: list[str] = []

        if engagement_declining and duration_declining:
            score += 0.15
            reasons.append("engagement and duration both declining")
        elif engagement_declining:
            score += 0.10
            reasons.append("engagement declining")

        if bounce_rising:
            score += 0.10
            reasons.append("bounce rate rising")

        if trend_consistent:
            score += 0.10
            reasons.append("consistent downward trend")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
