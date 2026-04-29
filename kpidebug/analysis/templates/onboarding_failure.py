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

logger = logging.getLogger(__name__)

SIGNUP_RATE_ID = "builtin:ga.signup_rate"
SIGNUP_TO_PAID_ID = "builtin:ga.signup_to_paid_rate"
NEW_USERS_ID = "builtin:ga.new_users"
CUSTOMER_COUNT_ID = "builtin:stripe.customer_count"
GROSS_REVENUE_ID = "builtin:stripe.gross_revenue"

SIGNUPS_GROWTH_THRESHOLD = 0.05
S2P_DROP_THRESHOLD = -0.10

DESCRIPTION = (
    "More people are signing up, but a smaller share of them "
    "become paying customers. Your acquisition is working — "
    "the problem is somewhere between signup and first payment. "
    "Check onboarding steps, trial experience, or activation "
    "triggers."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


class OnboardingFailureTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        signup_dm = ctx.get_dashboard_metric(SIGNUP_RATE_ID)
        s2p_dm = ctx.get_dashboard_metric(SIGNUP_TO_PAID_ID)

        if signup_dm is None or signup_dm.snapshot is None:
            return None
        if s2p_dm is None or s2p_dm.snapshot is None:
            return None

        signup_change = signup_dm.snapshot.change(
            TREND_WINDOW_DAYS, signup_dm.aggregation,
        )
        s2p_change = s2p_dm.snapshot.change(
            TREND_WINDOW_DAYS, s2p_dm.aggregation,
        )

        if signup_change < SIGNUPS_GROWTH_THRESHOLD:
            return None
        if s2p_change > S2P_DROP_THRESHOLD:
            return None

        signals = [
            Signal(
                metric_id=SIGNUP_RATE_ID,
                description=(
                    f"Signup rate ↑ "
                    f"{signup_change * 100:.1f}%"
                ),
                value=signup_dm.snapshot.value,
                change=signup_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=SIGNUP_TO_PAID_ID,
                description=(
                    f"Signup-to-paid ↓ "
                    f"{abs(s2p_change) * 100:.1f}%"
                ),
                value=s2p_dm.snapshot.value,
                change=s2p_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        customer_dm = ctx.get_dashboard_metric(CUSTOMER_COUNT_ID)
        customer_confirms = False
        if customer_dm and customer_dm.snapshot:
            cust_change = customer_dm.snapshot.change(
                TREND_WINDOW_DAYS, customer_dm.aggregation,
            )
            cust_cat = classify_change(cust_change)
            if cust_cat in (
                ChangeCategory.SMALL_DROP,
                ChangeCategory.LARGE_DROP,
            ):
                customer_confirms = True
                signals.append(Signal(
                    metric_id=CUSTOMER_COUNT_ID,
                    description=(
                        f"New customers ↓ "
                        f"{abs(cust_change) * 100:.1f}%"
                    ),
                    value=customer_dm.snapshot.value,
                    change=cust_change,
                    period_days=TREND_WINDOW_DAYS,
                ))
            elif cust_cat == ChangeCategory.NEGLIGIBLE:
                signals.append(Signal(
                    metric_id=CUSTOMER_COUNT_ID,
                    description=(
                        f"New customers ~ flat "
                        f"despite more signups"
                    ),
                    value=customer_dm.snapshot.value,
                    change=cust_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        actions = [
            Action(
                description=(
                    "Review onboarding flow — are new signups "
                    "completing setup, seeing value, and "
                    "reaching the aha moment?"
                ),
                priority=Priority.HIGH,
            ),
            Action(
                description=(
                    "Check if signup quality changed — new "
                    "acquisition channel bringing less "
                    "qualified leads?"
                ),
                priority=Priority.MEDIUM,
            ),
        ]

        revenue_impact = self._estimate_revenue_impact(ctx)
        counterfactual = self._estimate_counterfactual(
            s2p_dm, ctx, revenue_impact,
        )
        confidence = self._compute_confidence(
            signup_change, s2p_change, customer_confirms,
        )

        return Insight(
            headline=(
                "More signups but fewer conversions to paid "
                "— onboarding is broken"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            counterfactual=counterfactual,
            revenue_impact=revenue_impact,
            confidence=confidence,
        )

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
        self, s2p_dm, ctx: AnalysisContext,
        revenue_impact: RevenueImpact,
    ) -> Counterfactual:
        snapshot = s2p_dm.snapshot
        if len(snapshot.values) < TREND_WINDOW_DAYS * 2:
            return Counterfactual()

        prev_vals = snapshot.values[
            -(TREND_WINDOW_DAYS * 2):-TREND_WINDOW_DAYS
        ]
        recent_vals = snapshot.values[-TREND_WINDOW_DAYS:]
        prev_rate = sum(prev_vals) / len(prev_vals)
        recent_rate = sum(recent_vals) / len(recent_vals)
        lost_pct = max(prev_rate - recent_rate, 0.0)

        new_users_dm = ctx.get_dashboard_metric(NEW_USERS_ID)
        recent_signups = 0.0
        if new_users_dm and new_users_dm.snapshot:
            recent_signups = new_users_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, new_users_dm.aggregation,
            )
        lost_customers = recent_signups * (lost_pct / 100.0)

        rev_recovery = RevenueImpact()
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_ID)
        if rev_dm and rev_dm.snapshot and lost_customers > 0:
            cust_dm = ctx.get_dashboard_metric(CUSTOMER_COUNT_ID)
            if cust_dm and cust_dm.snapshot:
                recent_customers = cust_dm.snapshot.aggregate_value(
                    TREND_WINDOW_DAYS, cust_dm.aggregation,
                )
                recent_rev = rev_dm.snapshot.aggregate_value(
                    TREND_WINDOW_DAYS, rev_dm.aggregation,
                ) / 100
                if recent_customers > 0:
                    arpu = recent_rev / recent_customers
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
            metric_name="Signup-to-Paid",
            description=(
                f"~{lost_customers:.0f} extra paying customers "
                f"at prior {prev_rate:.1f}% conversion"
            ),
            revenue_impact=rev_recovery,
        )

    def _compute_confidence(
        self,
        signup_change: float,
        s2p_change: float,
        customer_confirms: bool,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        if abs(s2p_change) >= 0.30:
            score += 0.15
            reasons.append("large s2p drop")
        elif abs(s2p_change) >= 0.15:
            score += 0.10
            reasons.append("moderate s2p drop")

        if signup_change >= 0.15:
            score += 0.10
            reasons.append("signups clearly rising")

        if customer_confirms:
            score += 0.10
            reasons.append("customer count confirms")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
