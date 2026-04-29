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

CONVERSION_RATE_ID = "builtin:ga.conversion_rate"
REV_PER_CONVERSION_ID = "builtin:ga.revenue_per_conversion"
GROSS_REVENUE_ID = "builtin:stripe.gross_revenue"
CUSTOMER_COUNT_ID = "builtin:stripe.customer_count"
MRR_ID = "builtin:stripe.mrr"
SESSIONS_ID = "builtin:ga.sessions"

DESCRIPTION = (
    "Fewer visitors are converting, but the ones who do are "
    "spending more. This usually means your pricing is too "
    "high for most of your audience — you're selecting for "
    "only the highest-willingness-to-pay customers and losing "
    "everyone else."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


class PricingMismatchTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        conv_dm = ctx.get_dashboard_metric(CONVERSION_RATE_ID)
        rpc_dm = ctx.get_dashboard_metric(REV_PER_CONVERSION_ID)

        if conv_dm is None or conv_dm.snapshot is None:
            return None
        if rpc_dm is None or rpc_dm.snapshot is None:
            return None

        conv_change = conv_dm.snapshot.change(
            TREND_WINDOW_DAYS, conv_dm.aggregation,
        )
        rpc_change = rpc_dm.snapshot.change(
            TREND_WINDOW_DAYS, rpc_dm.aggregation,
        )

        conv_cat = classify_change(conv_change)
        rpc_cat = classify_change(rpc_change)

        conv_dropping = conv_cat in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        )
        rpc_rising = rpc_cat in (
            ChangeCategory.SMALL_GAIN, ChangeCategory.LARGE_GAIN,
        )

        if not conv_dropping or not rpc_rising:
            return None

        signals = [
            Signal(
                metric_id=CONVERSION_RATE_ID,
                description=(
                    f"Conversion ↓ "
                    f"{abs(conv_change) * 100:.1f}%"
                ),
                value=conv_dm.snapshot.value,
                change=conv_change,
                period_days=TREND_WINDOW_DAYS,
            ),
            Signal(
                metric_id=REV_PER_CONVERSION_ID,
                description=(
                    f"Revenue/conversion ↑ "
                    f"{rpc_change * 100:.1f}%"
                ),
                value=rpc_dm.snapshot.value,
                change=rpc_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        customer_dm = ctx.get_dashboard_metric(CUSTOMER_COUNT_ID)
        customer_dropping = False
        if customer_dm and customer_dm.snapshot:
            cust_change = customer_dm.snapshot.change(
                TREND_WINDOW_DAYS, customer_dm.aggregation,
            )
            cust_cat = classify_change(cust_change)
            if cust_cat in (
                ChangeCategory.SMALL_DROP,
                ChangeCategory.LARGE_DROP,
            ):
                customer_dropping = True
                signals.append(Signal(
                    metric_id=CUSTOMER_COUNT_ID,
                    description=(
                        f"Customers ↓ "
                        f"{abs(cust_change) * 100:.1f}%"
                    ),
                    value=customer_dm.snapshot.value,
                    change=cust_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        mrr_dm = ctx.get_dashboard_metric(MRR_ID)
        mrr_stable = False
        if mrr_dm and mrr_dm.snapshot:
            mrr_change = mrr_dm.snapshot.change(
                TREND_WINDOW_DAYS, mrr_dm.aggregation,
            )
            mrr_cat = classify_change(mrr_change)
            if mrr_cat == ChangeCategory.NEGLIGIBLE:
                mrr_stable = True
                signals.append(Signal(
                    metric_id=MRR_ID,
                    description=(
                        f"MRR ~ stable ({mrr_change * 100:+.1f}%) "
                        f"— fewer but higher-value customers"
                    ),
                    value=mrr_dm.snapshot.value,
                    change=mrr_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        actions = [
            Action(
                description=(
                    "Review pricing tiers — consider adding "
                    "a lower entry-point plan or freemium tier"
                ),
                priority=Priority.HIGH,
            ),
            Action(
                description=(
                    "A/B test pricing page to measure "
                    "price sensitivity"
                ),
                priority=Priority.MEDIUM,
            ),
        ]

        revenue_impact = self._estimate_revenue_impact(ctx)
        counterfactual = self._estimate_counterfactual(
            conv_dm, ctx, revenue_impact,
        )
        confidence = self._compute_confidence(
            conv_change, rpc_change,
            customer_dropping, mrr_stable,
        )

        return Insight(
            headline=(
                "Conversion dropping while spend per customer "
                "rises — pricing may be too high"
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
        self, conv_dm, ctx: AnalysisContext,
        revenue_impact: RevenueImpact,
    ) -> Counterfactual:
        snapshot = conv_dm.snapshot
        if len(snapshot.values) < TREND_WINDOW_DAYS * 2:
            return Counterfactual()

        prev_vals = snapshot.values[
            -(TREND_WINDOW_DAYS * 2):-TREND_WINDOW_DAYS
        ]
        recent_vals = snapshot.values[-TREND_WINDOW_DAYS:]
        prev_rate = sum(prev_vals) / len(prev_vals)
        recent_rate = sum(recent_vals) / len(recent_vals)
        lost_pct = max(prev_rate - recent_rate, 0.0)

        sessions_dm = ctx.get_dashboard_metric(SESSIONS_ID)
        recent_sessions = 0.0
        if sessions_dm and sessions_dm.snapshot:
            from kpidebug.data.types import Aggregation
            recent_sessions = sessions_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, Aggregation.SUM,
            )
        lost_conversions = recent_sessions * (lost_pct / 100.0)

        rev_recovery = RevenueImpact()
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_ID)
        if rev_dm and rev_dm.snapshot and lost_conversions > 0:
            recent_rev = rev_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, rev_dm.aggregation,
            ) / 100
            current_conv = recent_rate / 100.0
            total_conv = recent_sessions * current_conv
            if total_conv > 0:
                avg_rev = recent_rev / total_conv
                recoverable = lost_conversions * avg_rev
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
            value=lost_conversions,
            metric_id=CONVERSION_RATE_ID,
            metric_name="Conversions",
            description=(
                f"~{lost_conversions:.0f} conversions "
                f"recoverable at prior {prev_rate:.1f}% rate"
            ),
            revenue_impact=rev_recovery,
        )

    def _compute_confidence(
        self,
        conv_change: float,
        rpc_change: float,
        customer_dropping: bool,
        mrr_stable: bool,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        if abs(conv_change) >= 0.15 and rpc_change >= 0.15:
            score += 0.15
            reasons.append("strong opposing signals")
        else:
            score += 0.05
            reasons.append("moderate opposing signals")

        if customer_dropping:
            score += 0.10
            reasons.append("customer count also declining")

        if mrr_stable:
            score += 0.10
            reasons.append("MRR stable despite fewer customers")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
