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

COLLECTION_RATE_ID = "builtin:stripe.invoice_collection_rate"
GROSS_REVENUE_ID = "builtin:stripe.gross_revenue"
SESSIONS_METRIC_ID = "builtin:ga.sessions"
MRR_METRIC_ID = "builtin:stripe.mrr"
FAILED_PAYMENTS_ID = "builtin:stripe.failed_payment_count"
CHURN_COUNT_ID = "builtin:stripe.churn_count"

DESCRIPTION = (
    "Payments are failing at a higher rate than usual while "
    "your customers are still actively using the product. "
    "This is involuntary churn — customers who want to pay "
    "but can't. Usually fixable by retrying payments, "
    "updating card details, or improving dunning flows."
)


def _fmt_dollars(v: float) -> str:
    if abs(v) >= 1_000_000:
        return f"${v / 1_000_000:,.1f}M"
    if abs(v) >= 1_000:
        return f"${v / 1_000:,.1f}k"
    return f"${v:,.0f}"


class InvoluntaryChurnTemplate(InsightTemplate):

    def evaluate(self, ctx: AnalysisContext) -> Insight | None:
        collection_dm = ctx.get_dashboard_metric(COLLECTION_RATE_ID)
        if collection_dm is None or collection_dm.snapshot is None:
            return None

        collection_change = collection_dm.snapshot.change(
            TREND_WINDOW_DAYS, collection_dm.aggregation,
        )
        collection_cat = classify_change(collection_change)
        if collection_cat not in (
            ChangeCategory.SMALL_DROP, ChangeCategory.LARGE_DROP,
        ):
            return None

        sessions_dm = ctx.get_dashboard_metric(SESSIONS_METRIC_ID)
        activity_stable = False
        if sessions_dm and sessions_dm.snapshot:
            sess_change = sessions_dm.snapshot.change(
                TREND_WINDOW_DAYS, sessions_dm.aggregation,
            )
            activity_stable = abs(sess_change) < NEGLIGIBLE_THRESHOLD

        signals = [
            Signal(
                metric_id=COLLECTION_RATE_ID,
                description=(
                    f"Invoice collection ↓ "
                    f"{abs(collection_change) * 100:.1f}%"
                ),
                value=collection_dm.snapshot.value,
                change=collection_change,
                period_days=TREND_WINDOW_DAYS,
            ),
        ]

        if sessions_dm and sessions_dm.snapshot:
            sess_change = sessions_dm.snapshot.change(
                TREND_WINDOW_DAYS, sessions_dm.aggregation,
            )
            signals.append(Signal(
                metric_id=SESSIONS_METRIC_ID,
                description=(
                    f"User activity ~ stable "
                    f"({sess_change * 100:+.1f}%)"
                    if activity_stable else
                    f"User activity "
                    f"{sess_change * 100:+.1f}%"
                ),
                value=sessions_dm.snapshot.value,
                change=sess_change,
                period_days=TREND_WINDOW_DAYS,
            ))

        failed_dm = ctx.get_dashboard_metric(FAILED_PAYMENTS_ID)
        if failed_dm and failed_dm.snapshot:
            fp_change = failed_dm.snapshot.change(
                TREND_WINDOW_DAYS, failed_dm.aggregation,
            )
            fp_cat = classify_change(fp_change)
            if fp_cat in (
                ChangeCategory.SMALL_GAIN,
                ChangeCategory.LARGE_GAIN,
            ):
                signals.append(Signal(
                    metric_id=FAILED_PAYMENTS_ID,
                    description=(
                        f"Failed payments ↑ "
                        f"{fp_change * 100:.1f}%"
                    ),
                    value=failed_dm.snapshot.value,
                    change=fp_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        churn_dm = ctx.get_dashboard_metric(CHURN_COUNT_ID)
        if churn_dm and churn_dm.snapshot:
            churn_change = churn_dm.snapshot.change(
                TREND_WINDOW_DAYS, churn_dm.aggregation,
            )
            churn_cat = classify_change(churn_change)
            if churn_cat in (
                ChangeCategory.SMALL_GAIN,
                ChangeCategory.LARGE_GAIN,
            ):
                signals.append(Signal(
                    metric_id=CHURN_COUNT_ID,
                    description=(
                        f"Churn ↑ "
                        f"{churn_change * 100:.1f}%"
                    ),
                    value=churn_dm.snapshot.value,
                    change=churn_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        mrr_dm = ctx.get_dashboard_metric(MRR_METRIC_ID)
        mrr_dropping = False
        if mrr_dm and mrr_dm.snapshot:
            mrr_change = mrr_dm.snapshot.change(
                TREND_WINDOW_DAYS, mrr_dm.aggregation,
            )
            mrr_cat = classify_change(mrr_change)
            if mrr_cat in (
                ChangeCategory.SMALL_DROP,
                ChangeCategory.LARGE_DROP,
            ):
                mrr_dropping = True
                signals.append(Signal(
                    metric_id=MRR_METRIC_ID,
                    description=(
                        f"MRR ↓ "
                        f"{abs(mrr_change) * 100:.1f}%"
                    ),
                    value=mrr_dm.snapshot.value,
                    change=mrr_change,
                    period_days=TREND_WINDOW_DAYS,
                ))

        actions = [
            Action(
                description=(
                    "Review failed payment reasons — expired "
                    "cards, insufficient funds, or gateway "
                    "errors"
                ),
                priority=Priority.HIGH,
            ),
            Action(
                description=(
                    "Improve dunning flow — retry timing, "
                    "card update reminders, grace periods"
                ),
                priority=Priority.HIGH,
            ),
        ]

        revenue_impact = self._estimate_revenue_impact(ctx)
        counterfactual = self._estimate_counterfactual(
            collection_dm, ctx, revenue_impact,
        )
        confidence = self._compute_confidence(
            collection_change, activity_stable, mrr_dropping,
        )

        return Insight(
            headline=(
                "Payment failures rising while customers "
                "stay active — involuntary churn"
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
        self, collection_dm, ctx: AnalysisContext,
        revenue_impact: RevenueImpact,
    ) -> Counterfactual:
        snapshot = collection_dm.snapshot
        if len(snapshot.values) < TREND_WINDOW_DAYS * 2:
            return Counterfactual()

        prev_vals = snapshot.values[
            -(TREND_WINDOW_DAYS * 2):-TREND_WINDOW_DAYS
        ]
        recent_vals = snapshot.values[-TREND_WINDOW_DAYS:]
        prev_rate = sum(prev_vals) / len(prev_vals)
        recent_rate = sum(recent_vals) / len(recent_vals)
        lost_pct = max(prev_rate - recent_rate, 0.0)

        rev_recovery = RevenueImpact()
        rev_dm = ctx.get_dashboard_metric(GROSS_REVENUE_ID)
        if rev_dm and rev_dm.snapshot and lost_pct > 0:
            recent_rev = rev_dm.snapshot.aggregate_value(
                TREND_WINDOW_DAYS, rev_dm.aggregation,
            ) / 100
            recoverable = recent_rev * (lost_pct / 100.0)
            if revenue_impact.value > 0:
                recoverable = min(recoverable, revenue_impact.value)
            rev_recovery = RevenueImpact(
                value=recoverable,
                description=(
                    f"Recovery: ~{_fmt_dollars(recoverable)} (7d)"
                ),
            )

        return Counterfactual(
            value=lost_pct,
            metric_id=COLLECTION_RATE_ID,
            metric_name="Collection Rate",
            description=(
                f"~{lost_pct:.1f}% collection rate "
                f"recoverable with better dunning"
            ),
            revenue_impact=rev_recovery,
        )

    def _compute_confidence(
        self,
        collection_change: float,
        activity_stable: bool,
        mrr_dropping: bool,
    ) -> Confidence:
        score = 0.5
        reasons: list[str] = []

        if abs(collection_change) >= 0.15:
            score += 0.15
            reasons.append("large collection rate drop")
        else:
            score += 0.05
            reasons.append("moderate collection rate drop")

        if activity_stable:
            score += 0.10
            reasons.append("user activity stable")

        if mrr_dropping:
            score += 0.10
            reasons.append("MRR confirms revenue loss")

        score = min(score, 1.0)
        return Confidence(
            score=score,
            description=", ".join(reasons).capitalize(),
        )
