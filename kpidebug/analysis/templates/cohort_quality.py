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

SIGNUP_TO_PAID_ID = "builtin:ga.signup_to_paid_rate"
NEW_USERS_ID = "builtin:ga.new_users"
CUSTOMER_COUNT_ID = "builtin:stripe.customer_count"
GROSS_REVENUE_ID = "builtin:stripe.gross_revenue"
RETENTION_30D_ID = "builtin:stripe.retention_30d"

QUALITY_THRESHOLD = 0.70

DESCRIPTION = (
    "Recent signups are converting to paid at a much lower "
    "rate than your historical average. The people finding "
    "you lately are less likely to become customers — either "
    "your acquisition channels are attracting the wrong "
    "audience or something changed in your market positioning."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


class CohortQualityTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        s2p_dm = ctx.get_dashboard_metric(SIGNUP_TO_PAID_ID)
        if s2p_dm is None or s2p_dm.snapshot is None:
            return None

        snapshot = s2p_dm.snapshot
        if len(snapshot.values) < COMPARISON_WINDOW_DAYS:
            return None

        recent_avg = (
            sum(snapshot.values[-TREND_WINDOW_DAYS:])
            / TREND_WINDOW_DAYS
        )
        historical_avg = (
            sum(snapshot.values[-COMPARISON_WINDOW_DAYS:])
            / COMPARISON_WINDOW_DAYS
        )

        if historical_avg <= 0:
            return None

        ratio = recent_avg / historical_avg
        if ratio >= QUALITY_THRESHOLD:
            return None

        gap_pct = (1.0 - ratio) * 100

        signals = [
            Signal(
                metric_id=SIGNUP_TO_PAID_ID,
                description=(
                    f"Signup-to-paid {recent_avg:.1f}% "
                    f"vs {historical_avg:.1f}% avg "
                    f"(↓ {gap_pct:.0f}%)"
                ),
                value=recent_avg,
                change=ratio - 1.0,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        ret_dm = ctx.get_dashboard_metric(RETENTION_30D_ID)
        if ret_dm and ret_dm.snapshot:
            ret_snap = ret_dm.snapshot
            if len(ret_snap.values) >= COMPARISON_WINDOW_DAYS:
                recent_ret = (
                    sum(ret_snap.values[-TREND_WINDOW_DAYS:])
                    / TREND_WINDOW_DAYS
                )
                historical_ret = (
                    sum(ret_snap.values[-COMPARISON_WINDOW_DAYS:])
                    / COMPARISON_WINDOW_DAYS
                )
                if historical_ret > 0:
                    ret_change = (
                        (recent_ret - historical_ret)
                        / historical_ret
                    )
                    ret_cat = classify_change(ret_change)
                    if ret_cat in (
                        ChangeCategory.SMALL_DROP,
                        ChangeCategory.LARGE_DROP,
                    ):
                        signals.append(Signal(
                            metric_id=RETENTION_30D_ID,
                            description=(
                                f"30d retention {recent_ret:.1f}% "
                                f"vs {historical_ret:.1f}% avg "
                                f"(↓ {abs(ret_change) * 100:.0f}%)"
                            ),
                            value=recent_ret,
                            change=ret_change,
                            period_days=TREND_WINDOW_DAYS,
                        ))

        new_users_dm = ctx.get_dashboard_metric(NEW_USERS_ID)
        new_users_rising = False
        if new_users_dm and new_users_dm.snapshot:
            nu_change = new_users_dm.snapshot.change(
                TREND_WINDOW_DAYS, new_users_dm.aggregation,
            )
            nu_cat = classify_change(nu_change)
            if nu_cat in (
                ChangeCategory.SMALL_GAIN,
                ChangeCategory.LARGE_GAIN,
            ):
                new_users_rising = True
                signals.append(Signal(
                    metric_id=NEW_USERS_ID,
                    description=(
                        f"New users ↑ "
                        f"{nu_change * 100:.1f}% "
                        f"— more volume, lower quality"
                    ),
                    value=new_users_dm.snapshot.value,
                    change=nu_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        multi_week = self._check_multi_week_decline(snapshot)

        actions = [
            Action(
                description=(
                    "Audit acquisition channels — which "
                    "sources are bringing lower-quality leads?"
                ),
                priority=Priority.HIGH,
            ),
            Action(
                description=(
                    "Review landing pages and ad targeting — "
                    "are you reaching the right audience?"
                ),
                priority=Priority.MEDIUM,
            ),
        ]

        revenue_impact = self._estimate_revenue_impact(ctx)
        counterfactual = self._estimate_counterfactual(
            recent_avg, historical_avg, ctx, revenue_impact,
        )
        confidence = self._compute_confidence(
            gap_pct, new_users_rising, multi_week,
        )

        return Insight(
            headline=(
                "Recent signups converting at "
                f"{gap_pct:.0f}% below historical rate "
                "— acquisition quality declining"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            counterfactual=counterfactual,
            revenue_impact=revenue_impact,
            confidence=confidence,
        )

    def _check_multi_week_decline(
        self, snapshot,
    ) -> bool:
        vals = snapshot.values
        if len(vals) < TREND_WINDOW_DAYS * 3:
            return False
        w1 = sum(vals[-TREND_WINDOW_DAYS * 3:-TREND_WINDOW_DAYS * 2]) / TREND_WINDOW_DAYS
        w2 = sum(vals[-TREND_WINDOW_DAYS * 2:-TREND_WINDOW_DAYS]) / TREND_WINDOW_DAYS
        w3 = sum(vals[-TREND_WINDOW_DAYS:]) / TREND_WINDOW_DAYS
        return w1 > w2 > w3

    def _estimate_revenue_impact(
        self, ctx: AnalysisContext,
    ) -> RevenueImpact:
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_ID)
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

    def _estimate_counterfactual(
        self,
        recent_rate: float,
        historical_rate: float,
        ctx: AnalysisContext,
        revenue_impact: RevenueImpact,
    ) -> Counterfactual:
        lost_pct = historical_rate - recent_rate
        new_users_dm = ctx.get_dashboard_metric(NEW_USERS_ID)
        if new_users_dm is None or new_users_dm.snapshot is None:
            return Counterfactual()

        from kpidebug.data.types import Aggregation
        recent_signups = new_users_dm.snapshot.aggregate_value(
            TREND_WINDOW_DAYS, Aggregation.SUM,
        )
        lost_customers = recent_signups * (lost_pct / 100.0)

        rev_recovery = RevenueImpact()
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_ID)
        cust_dm = ctx.get_dashboard_metric(CUSTOMER_COUNT_ID)
        if (
            rev_dm and rev_dm.snapshot
            and cust_dm and cust_dm.snapshot
            and lost_customers > 0
        ):
            recent_rev = rev_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, rev_dm.aggregation,
            ) / 100
            recent_cust = cust_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, cust_dm.aggregation,
            )
            if recent_cust > 0:
                arpu = recent_rev / recent_cust
                recoverable = lost_customers * arpu
                if revenue_impact.value > 0:
                    recoverable = min(
                        recoverable, revenue_impact.value,
                    )
                rev_recovery = RevenueImpact(
                    value=recoverable,
                    description=(
                        f"Recovery: "
                        f"~{_fmt_dollars(recoverable)} (7d)"
                    ),
                )

        return Counterfactual(
            value=lost_customers,
            metric_id=SIGNUP_TO_PAID_ID,
            metric_name="Customers",
            description=(
                f"~{lost_customers:.0f} extra customers "
                f"at historical {historical_rate:.1f}% rate"
            ),
            revenue_impact=rev_recovery,
        )

    def _compute_confidence(
        self,
        gap_pct: float,
        new_users_rising: bool,
        multi_week: bool,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        if gap_pct >= 40:
            score += 0.15
            reasons.append(f"{gap_pct:.0f}% below historical")
        else:
            score += 0.05
            reasons.append(f"{gap_pct:.0f}% below historical")

        if new_users_rising:
            score += 0.10
            reasons.append("volume up but quality down")

        if multi_week:
            score += 0.10
            reasons.append("declining for multiple weeks")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
