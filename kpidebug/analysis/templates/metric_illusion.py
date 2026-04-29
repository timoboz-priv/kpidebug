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

logger = logging.getLogger(__name__)

SESSIONS_ID = "builtin:ga.sessions"
NEW_USERS_ID = "builtin:ga.new_users"
CONVERSION_RATE_ID = "builtin:ga.conversion_rate"
GROSS_REVENUE_ID = "builtin:stripe.gross_revenue"
CUSTOMER_COUNT_ID = "builtin:stripe.customer_count"

DESCRIPTION = (
    "Your traffic and signups are growing, but revenue isn't "
    "keeping up. You're attracting more visitors without "
    "turning them into paying customers. This is vanity "
    "growth — it looks good on a dashboard but doesn't "
    "translate to business results."
)


class MetricIllusionTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        sessions_dm = ctx.get_dashboard_metric(SESSIONS_ID)
        new_users_dm = ctx.get_dashboard_metric(NEW_USERS_ID)
        revenue_dm = ctx.get_dashboard_metric(GROSS_REVENUE_ID)

        if sessions_dm is None or sessions_dm.snapshot is None:
            return None
        if new_users_dm is None or new_users_dm.snapshot is None:
            return None
        if revenue_dm is None or revenue_dm.snapshot is None:
            return None

        sessions_change = sessions_dm.snapshot.change(
            TREND_WINDOW_DAYS, sessions_dm.aggregation,
        )
        new_users_change = new_users_dm.snapshot.change(
            TREND_WINDOW_DAYS, new_users_dm.aggregation,
        )
        revenue_change = revenue_dm.snapshot.change(
            TREND_WINDOW_DAYS, revenue_dm.aggregation,
        )

        sess_cat = classify_change(sessions_change)
        nu_cat = classify_change(new_users_change)
        rev_cat = classify_change(revenue_change)

        top_funnel_growing = (
            sess_cat in (ChangeCategory.SMALL_GAIN, ChangeCategory.LARGE_GAIN)
            and nu_cat in (ChangeCategory.SMALL_GAIN, ChangeCategory.LARGE_GAIN)
        )
        revenue_flat_or_down = rev_cat in (
            ChangeCategory.NEGLIGIBLE,
            ChangeCategory.SMALL_DROP,
            ChangeCategory.LARGE_DROP,
        )

        if not top_funnel_growing or not revenue_flat_or_down:
            return None

        signals = [
            Signal(
                metric_id=SESSIONS_ID,
                description=(
                    f"Sessions ↑ "
                    f"{sessions_change * 100:.1f}%"
                ),
                value=sessions_dm.snapshot.value,
                change=sessions_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=NEW_USERS_ID,
                description=(
                    f"New users ↑ "
                    f"{new_users_change * 100:.1f}%"
                ),
                value=new_users_dm.snapshot.value,
                change=new_users_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=GROSS_REVENUE_ID,
                description=(
                    f"Revenue ~ flat "
                    f"({revenue_change * 100:+.1f}%)"
                    if rev_cat == ChangeCategory.NEGLIGIBLE else
                    f"Revenue ↓ "
                    f"{abs(revenue_change) * 100:.1f}%"
                ),
                value=revenue_dm.snapshot.value,
                change=revenue_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        conv_dm = ctx.get_dashboard_metric(CONVERSION_RATE_ID)
        conv_dropping = False
        if conv_dm and conv_dm.snapshot:
            conv_change = conv_dm.snapshot.change(
                TREND_WINDOW_DAYS, conv_dm.aggregation,
            )
            conv_cat = classify_change(conv_change)
            if conv_cat in (
                ChangeCategory.SMALL_DROP,
                ChangeCategory.LARGE_DROP,
            ):
                conv_dropping = True
                signals.append(Signal(
                    metric_id=CONVERSION_RATE_ID,
                    description=(
                        f"Conversion ↓ "
                        f"{abs(conv_change) * 100:.1f}%"
                    ),
                    value=conv_dm.snapshot.value,
                    change=conv_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        cust_dm = ctx.get_dashboard_metric(CUSTOMER_COUNT_ID)
        cust_stagnant = False
        if cust_dm and cust_dm.snapshot:
            cust_change = cust_dm.snapshot.change(
                TREND_WINDOW_DAYS, cust_dm.aggregation,
            )
            cust_cat = classify_change(cust_change)
            if cust_cat in (
                ChangeCategory.NEGLIGIBLE,
                ChangeCategory.SMALL_DROP,
                ChangeCategory.LARGE_DROP,
            ):
                cust_stagnant = True
                signals.append(Signal(
                    metric_id=CUSTOMER_COUNT_ID,
                    description=(
                        f"Customers "
                        f"({cust_change * 100:+.1f}%) "
                        f"— not keeping up with traffic"
                    ),
                    value=cust_dm.snapshot.value,
                    change=cust_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        actions = [
            Action(
                description=(
                    "Focus on conversion, not traffic — "
                    "more visitors without monetization "
                    "is wasted spend"
                ),
                priority=Priority.HIGH,
            ),
            Action(
                description=(
                    "Audit acquisition channels — which "
                    "ones drive traffic but no revenue?"
                ),
                priority=Priority.HIGH,
            ),
        ]

        revenue_impact = RevenueImpact()
        if rev_cat in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        ):
            recent = revenue_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, revenue_dm.aggregation,
            )
            previous = (
                revenue_dm.snapshot.aggregate_value(
                    TREND_WINDOW_DAYS * 2, revenue_dm.aggregation,
                ) - recent
            )
            lost = max(previous - recent, 0.0) / 100
            if lost > 0:
                revenue_impact = RevenueImpact(
                    value=lost,
                    description=(
                        f"Impact: revenue declining despite "
                        f"{sessions_change * 100:.0f}% more traffic"
                    ),
                )

        confirming = sum([
            conv_dropping, cust_stagnant,
            rev_cat in (ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP),
        ])

        counterfactual = Counterfactual(
            value=sessions_change * 100,
            metric_id=SESSIONS_ID,
            metric_name="Growth",
            description=(
                f"Traffic ↑ {sessions_change * 100:.0f}% "
                f"but revenue {revenue_change * 100:+.1f}% "
                f"— growth isn't monetizing"
            ),
        )
        confidence = self._compute_confidence(
            revenue_change, conv_dropping, confirming,
        )

        return Insight(
            headline=(
                "Traffic and signups growing but revenue "
                "isn't following — vanity growth"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            counterfactual=counterfactual,
            revenue_impact=revenue_impact,
            confidence=confidence,
        )

    def _compute_confidence(
        self,
        revenue_change: float,
        conv_dropping: bool,
        confirming_count: int,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        rev_cat = classify_change(revenue_change)
        if rev_cat in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        ):
            score += 0.15
            reasons.append("revenue actually declining")
        else:
            score += 0.05
            reasons.append("revenue flat despite growth")

        if conv_dropping:
            score += 0.10
            reasons.append("conversion rate dropping")

        if confirming_count >= 3:
            score += 0.10
            reasons.append(f"{confirming_count} confirming signals")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
