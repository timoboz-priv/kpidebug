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
from kpidebug.metrics.types import MetricSnapshot

logger = logging.getLogger(__name__)

SESSIONS_METRIC_ID = "builtin:ga.sessions"
CONVERSION_RATE_METRIC_ID = "builtin:ga.conversion_rate"
CONVERSIONS_METRIC_ID = "builtin:ga.conversions"
GROSS_REVENUE_METRIC_ID = "builtin:stripe.gross_revenue"
CUSTOMER_COUNT_METRIC_ID = "builtin:stripe.customer_count"
SIGNUP_RATE_METRIC_ID = "builtin:ga.signup_rate"
SIGNUP_TO_PAID_METRIC_ID = "builtin:ga.signup_to_paid_rate"

DESCRIPTION = (
    "Detects when conversion rate drops while traffic remains "
    "stable, indicating the problem is in the funnel or product "
    "rather than in acquisition. Correlates with revenue and "
    "customer metrics and locates the leakiest funnel step by "
    "comparing signup rate against signup-to-paid rate."
)


class ConversionBreakdownTemplate(InsightTemplate):

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

        traffic_stable = (
            abs(sessions_change) < NEGLIGIBLE_THRESHOLD
        )
        conversion_category = classify_change(conversion_change)
        is_significant_drop = conversion_category in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        )

        if not traffic_stable or not is_significant_drop:
            return None

        signals = [
            Signal(
                metric_id=CONVERSION_RATE_METRIC_ID,
                description=(
                    f"Conversion rate dropped "
                    f"{abs(conversion_change) * 100:.1f}% "
                    f"over {TREND_WINDOW_DAYS}d"
                ),
                value=conversion_snapshot.value,
                change=conversion_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=SESSIONS_METRIC_ID,
                description=(
                    f"Traffic stable "
                    f"({sessions_change * 100:+.1f}%)"
                ),
                value=sessions_snapshot.value,
                change=sessions_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        actions: list[Action] = []
        self._add_corroborating_signals(ctx, signals)
        self._add_funnel_analysis(ctx, signals, actions)

        if not actions:
            actions.append(Action(
                description=(
                    "Review conversion funnel for UX issues, "
                    "broken flows, or pricing changes"
                ),
                priority=Priority.HIGH,
            ))

        upside = self._estimate_upside(conversion_snapshot)

        return Insight(
            headline=(
                "Conversion drop with stable traffic "
                "— the funnel is leaking"
            ),
            description=DESCRIPTION,
            signals=signals,
            actions=actions,
            upside_potential=upside,
        )

    def _add_corroborating_signals(
        self,
        ctx: AnalysisContext,
        signals: list[Signal],
    ) -> None:
        for metric_id, label in [
            (CONVERSIONS_METRIC_ID, "Absolute conversions"),
            (GROSS_REVENUE_METRIC_ID, "Gross revenue"),
            (CUSTOMER_COUNT_METRIC_ID, "New customers"),
        ]:
            dm = ctx.get_dashboard_metric(metric_id)
            if dm is None or dm.snapshot is None:
                continue
            change = dm.snapshot.change(
                TREND_WINDOW_DAYS, dm.aggregation,
            )
            category = classify_change(change)
            if category in (
                ChangeCategory.SMALL_DROP,
                ChangeCategory.LARGE_DROP,
            ):
                signals.append(Signal(
                    metric_id=metric_id,
                    description=(
                        f"{label} also down "
                        f"{abs(change) * 100:.1f}%"
                    ),
                    value=dm.snapshot.value,
                    change=change,
                    period_days=TREND_WINDOW_DAYS,
                ))

    def _add_funnel_analysis(
        self,
        ctx: AnalysisContext,
        signals: list[Signal],
        actions: list[Action],
    ) -> None:
        signup_dm = ctx.get_dashboard_metric(
            SIGNUP_RATE_METRIC_ID,
        )
        s2p_dm = ctx.get_dashboard_metric(
            SIGNUP_TO_PAID_METRIC_ID,
        )

        if signup_dm and signup_dm.snapshot and s2p_dm and s2p_dm.snapshot:
            self._analyze_funnel_steps(
                signup_dm, s2p_dm, signals, actions,
            )
            return

        self._analyze_funnel_approximate(ctx, actions)

    def _analyze_funnel_steps(
        self,
        signup_dm,
        s2p_dm,
        signals: list[Signal],
        actions: list[Action],
    ) -> None:
        signup_change = signup_dm.snapshot.change(
            TREND_WINDOW_DAYS, signup_dm.aggregation,
        )
        s2p_change = s2p_dm.snapshot.change(
            TREND_WINDOW_DAYS, s2p_dm.aggregation,
        )

        signup_cat = classify_change(signup_change)
        s2p_cat = classify_change(s2p_change)

        signup_dropping = signup_cat in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        )
        s2p_dropping = s2p_cat in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        )

        steps = {
            "visitor_to_signup": (
                signup_dm.snapshot.value,
                signup_change,
                signup_dropping,
            ),
            "signup_to_paid": (
                s2p_dm.snapshot.value,
                s2p_change,
                s2p_dropping,
            ),
        }

        if signup_dropping:
            signals.append(Signal(
                metric_id=SIGNUP_RATE_METRIC_ID,
                description=(
                    f"Signup rate dropped "
                    f"{abs(signup_change) * 100:.1f}%"
                ),
                value=signup_dm.snapshot.value,
                change=signup_change,
                period_days=TREND_WINDOW_DAYS,
            ))

        if s2p_dropping:
            signals.append(Signal(
                metric_id=SIGNUP_TO_PAID_METRIC_ID,
                description=(
                    f"Signup-to-paid rate dropped "
                    f"{abs(s2p_change) * 100:.1f}%"
                ),
                value=s2p_dm.snapshot.value,
                change=s2p_change,
                period_days=TREND_WINDOW_DAYS,
            ))

        if signup_dropping and not s2p_dropping:
            actions.append(Action(
                description=(
                    "Signup rate is the weakest step — "
                    "investigate signup UX, landing pages, "
                    "or onboarding friction"
                ),
                priority=Priority.HIGH,
            ))
        elif s2p_dropping and not signup_dropping:
            actions.append(Action(
                description=(
                    "Signup-to-paid is the weakest step — "
                    "investigate payment flow, pricing page, "
                    "or trial-to-paid conversion"
                ),
                priority=Priority.HIGH,
            ))
        elif signup_dropping and s2p_dropping:
            worst_step = min(
                steps,
                key=lambda k: steps[k][1],
            )
            label = (
                "signup page" if worst_step == "visitor_to_signup"
                else "payment flow"
            )
            actions.append(Action(
                description=(
                    f"Both funnel steps declining, "
                    f"worst drop in {label} — "
                    f"start investigation there"
                ),
                priority=Priority.HIGH,
            ))

    def _analyze_funnel_approximate(
        self,
        ctx: AnalysisContext,
        actions: list[Action],
    ) -> None:
        conversions_dm = ctx.get_dashboard_metric(
            CONVERSIONS_METRIC_ID,
        )
        customers_dm = ctx.get_dashboard_metric(
            CUSTOMER_COUNT_METRIC_ID,
        )

        if conversions_dm is None or conversions_dm.snapshot is None:
            return
        if customers_dm is None or customers_dm.snapshot is None:
            return

        conv_change = conversions_dm.snapshot.change(
            TREND_WINDOW_DAYS, conversions_dm.aggregation,
        )
        cust_change = customers_dm.snapshot.change(
            TREND_WINDOW_DAYS, customers_dm.aggregation,
        )

        conv_dropping = classify_change(conv_change) in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        )
        cust_dropping = classify_change(cust_change) in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        )

        if conv_dropping and not cust_dropping:
            actions.append(Action(
                description=(
                    "Top-of-funnel conversions declining but "
                    "customers stable — check lead quality "
                    "or event tracking"
                ),
                priority=Priority.MEDIUM,
            ))
        elif cust_dropping and not conv_dropping:
            actions.append(Action(
                description=(
                    "Conversions holding but fewer customers "
                    "created — investigate signup-to-paid "
                    "flow or payment issues"
                ),
                priority=Priority.HIGH,
            ))
        elif conv_dropping and cust_dropping:
            actions.append(Action(
                description=(
                    "Both conversions and new customers "
                    "declining — review full funnel from "
                    "landing to payment"
                ),
                priority=Priority.HIGH,
            ))

    def _estimate_upside(
        self,
        conversion_snapshot: MetricSnapshot,
    ) -> UpsidePotential:
        if len(conversion_snapshot.values) < TREND_WINDOW_DAYS * 2:
            return UpsidePotential()
        previous_values = conversion_snapshot.values[
            -(TREND_WINDOW_DAYS * 2):-TREND_WINDOW_DAYS
        ]
        recent_values = conversion_snapshot.values[
            -TREND_WINDOW_DAYS:
        ]
        previous_avg = sum(previous_values) / len(previous_values)
        recent_avg = sum(recent_values) / len(recent_values)
        lost = max(previous_avg - recent_avg, 0.0)
        return UpsidePotential(
            value=lost,
            metric_id=CONVERSION_RATE_METRIC_ID,
            metric_name="Conversion Rate",
            description=(
                f"~{lost:.1f}pp conversion rate recoverable "
                f"by fixing the funnel"
            ),
        )
