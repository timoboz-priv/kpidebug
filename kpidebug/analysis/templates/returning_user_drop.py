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

logger = logging.getLogger(__name__)

USERS_BY_TYPE_ID = "builtin:ga.users_by_type"
SESSIONS_METRIC_ID = "builtin:ga.sessions"
ENGAGEMENT_RATE_ID = "builtin:ga.engagement_rate"
GROSS_REVENUE_METRIC_ID = "builtin:stripe.gross_revenue"
REVENUE_BY_USER_TYPE_ID = "builtin:stripe.revenue_by_user_type"

RETURNING_DROP_THRESHOLD = -0.20
NEW_USER_STABLE_THRESHOLD = 0.05

DESCRIPTION = (
    "Your returning users are disappearing while new user "
    "acquisition looks fine. This usually points to a product "
    "or experience problem — people try your product but "
    "don't come back."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


class ReturningUserDropTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        breakdown = self._get_user_type_breakdown(ctx)
        if breakdown is None:
            return None

        new_change, ret_change, ret_recent, ret_previous = breakdown

        if ret_change >= RETURNING_DROP_THRESHOLD:
            return None
        if abs(new_change) > NEW_USER_STABLE_THRESHOLD:
            return None

        signals = [
            Signal(
                metric_id=USERS_BY_TYPE_ID,
                description=(
                    f"Returning users ↓ "
                    f"{abs(ret_change) * 100:.1f}%"
                ),
                value=ret_recent,
                change=ret_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=USERS_BY_TYPE_ID,
                description=(
                    f"New users ~ stable "
                    f"({new_change * 100:+.1f}%)"
                ),
                value=0.0,
                change=new_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        engagement_dm = ctx.get_dashboard_metric(ENGAGEMENT_RATE_ID)
        engagement_dropping = False
        if engagement_dm and engagement_dm.snapshot:
            eng_change = engagement_dm.snapshot.change(
                TREND_WINDOW_DAYS, engagement_dm.aggregation,
            )
            eng_cat = classify_change(eng_change)
            if eng_cat in (
                ChangeCategory.SMALL_DROP,
                ChangeCategory.LARGE_DROP,
            ):
                engagement_dropping = True
                signals.append(Signal(
                    metric_id=ENGAGEMENT_RATE_ID,
                    description=(
                        f"Engagement ↓ "
                        f"{abs(eng_change) * 100:.1f}%"
                    ),
                    value=engagement_dm.snapshot.value,
                    change=eng_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        actions = [
            Action(
                description=(
                    "Investigate product changes, feature "
                    "removals, or UX regressions that may "
                    "have driven returning users away"
                ),
                priority=Priority.HIGH,
            ),
            Action(
                description=(
                    "Check email/notification engagement — "
                    "returning users depend on re-engagement"
                ),
                priority=Priority.MEDIUM,
            ),
        ]

        revenue_impact = self._estimate_revenue_impact(ctx)
        lost_users = max(ret_previous - ret_recent, 0.0)
        rev_recovery = self._estimate_returning_revenue_recovery(
            ctx, lost_users, ret_previous,
        )
        counterfactual = Counterfactual(
            value=lost_users,
            metric_id=USERS_BY_TYPE_ID,
            metric_name="Returning Users",
            description=(
                f"~{lost_users:.0f} returning users "
                f"recoverable"
            ),
            revenue_impact=rev_recovery,
        )
        confidence = self._compute_confidence(
            ret_change, new_change, engagement_dropping,
        )

        return Insight(
            headline=(
                "Returning users dropping while new "
                "acquisition holds — retention problem"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            counterfactual=counterfactual,
            revenue_impact=revenue_impact,
            confidence=confidence,
        )

    def _get_user_type_breakdown(
        self, ctx: AnalysisContext,
    ) -> tuple[float, float, float, float] | None:
        metric = ctx.get_metric(USERS_BY_TYPE_ID)
        if metric is None or not metric.dimensions:
            return None

        try:
            series = metric.compute_series(
                ctx._metric_context,
                dimensions=["new_vs_returning"],
                aggregation=Aggregation.SUM,
                days=TREND_WINDOW_DAYS * 2,
                date=ctx.as_of_date,
            )
        except Exception:
            return None

        type_recent: dict[str, float] = {}
        type_previous: dict[str, float] = {}
        midpoint = len(series.points) // 2

        for i, point in enumerate(series.points):
            for result in point.results:
                utype = (
                    next(iter(result.groups.values()), "unknown")
                    if result.groups else "unknown"
                )
                bucket = (
                    type_recent if i >= midpoint
                    else type_previous
                )
                bucket[utype] = (
                    bucket.get(utype, 0.0) + result.value
                )

        new_recent = type_recent.get("new", 0.0)
        new_previous = type_previous.get("new", 0.0)
        ret_recent = type_recent.get("returning", 0.0)
        ret_previous = type_previous.get("returning", 0.0)

        if new_previous == 0 or ret_previous == 0:
            return None

        new_change = (new_recent - new_previous) / abs(new_previous)
        ret_change = (ret_recent - ret_previous) / abs(ret_previous)

        return new_change, ret_change, ret_recent, ret_previous

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

    def _estimate_returning_revenue_recovery(
        self,
        ctx: AnalysisContext,
        lost_users: float,
        ret_previous: float,
    ) -> RevenueImpact:
        rev_metric = ctx.get_metric(REVENUE_BY_USER_TYPE_ID)
        if rev_metric is None or not rev_metric.dimensions:
            return RevenueImpact()

        try:
            series = rev_metric.compute_series(
                ctx._metric_context,
                dimensions=["user_type"],
                aggregation=Aggregation.SUM,
                days=TREND_WINDOW_DAYS * 2,
                date=ctx.as_of_date,
            )
        except Exception:
            return RevenueImpact()

        returning_rev = 0.0
        midpoint = len(series.points) // 2
        for i, point in enumerate(series.points):
            if i < midpoint:
                for result in point.results:
                    utype = result.groups.get("user_type", "")
                    if utype == "returning":
                        returning_rev += result.value

        if ret_previous <= 0 or returning_rev <= 0:
            return RevenueImpact()

        rev_per_user = returning_rev / ret_previous
        recoverable = lost_users * rev_per_user / 100

        return RevenueImpact(
            value=recoverable,
            description=(
                f"Recovery: ~{_fmt_dollars(recoverable)} "
                f"({TREND_WINDOW_DAYS}d)"
            ),
        )

    def _compute_confidence(
        self,
        ret_change: float,
        new_change: float,
        engagement_dropping: bool,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        if ret_change <= -0.30:
            score += 0.15
            reasons.append("large returning user drop")
        else:
            score += 0.05
            reasons.append("moderate returning user drop")

        new_stability = 1.0 - abs(new_change) / NEW_USER_STABLE_THRESHOLD
        if new_stability > 0.5:
            score += 0.10
            reasons.append("new users very stable")

        if engagement_dropping:
            score += 0.10
            reasons.append("engagement also declining")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
