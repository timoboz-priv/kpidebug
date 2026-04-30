from __future__ import annotations

import json
import logging
import re
import uuid
from datetime import datetime, timezone

from google.adk.agents import Agent

from kpidebug.analysis.analyzer import Analyzer
from kpidebug.common.agent import make_model, run_adk_agent
from kpidebug.analysis.context import AnalysisContext
from kpidebug.analysis.types import (
    Action,
    AnalysisResult,
    Confidence,
    Counterfactual,
    Insight,
    InsightSource,
    Priority,
    RevenueImpact,
    Signal,
)
from kpidebug.analysis.utils import TREND_WINDOW_DAYS
from kpidebug.data.types import Aggregation, FilterOperator

logger = logging.getLogger(__name__)

_context: AnalysisContext | None = None

GROSS_REVENUE_METRIC_ID = "builtin:stripe.gross_revenue"

DROP_FLAG_THRESHOLD = -0.05
MAX_TABLE_ROWS = 50
MAX_SEGMENTS = 30

AGENT_INSTRUCTION = """\
You are a KPI analyst investigating metric performance for a business.
Your job is to find significant metric changes, correlate them across
metrics, identify root causes, and produce actionable insights.

## Investigation methodology

You have a LIMITED budget of tool calls. Be efficient — do NOT call
the same tool twice, do NOT explore every dimension. Stop investigating
as soon as you have enough evidence to write insights.

Follow these steps:

1. Call `list_dashboard_metrics` to see ALL metrics and their 7d changes.
   Look at the FULL picture — not just revenue. Look for:
   - Metrics flagged with [!] (> 5% drop)
   - CONTRADICTIONS between metrics (traffic up but revenue down,
     conversion down but spend per customer up, signups up but
     paid customers down)
   - CORRELATED drops (multiple metrics declining together)

2. For the most important pattern you spotted, call `get_metric_detail`
   on 1-2 key metrics to see their dimensions.

3. Use `breakdown_metric` ONCE on the most relevant dimension to find
   which segment drives the change. Skip 'unknown' or 'unattributed'
   segments — they are not actionable.

4. Call `estimate_revenue_impact` once to quantify the business impact.

5. Output your insights immediately. Do NOT keep investigating.

## Patterns to look for

Think in terms of these business patterns:

- **Vanity growth**: Traffic/signups growing but revenue flat or down.
  The business looks healthy on surface metrics but isn't monetizing.
- **Pricing mismatch**: Conversion dropping while revenue per customer
  rises. Pricing may be too high for most visitors.
- **Funnel leak**: Traffic stable but conversion dropping. Something
  between visit and payment is broken (signup flow, pricing page, etc.)
- **Acquisition problem**: Traffic declining while conversion is stable.
  The product is fine but fewer people are finding it.
- **Involuntary churn**: Payment collection declining, failed invoices
  rising, while customers are still active. Payment infrastructure issue.
- **Onboarding failure**: Signups increasing but signup-to-paid rate
  dropping. New users aren't converting to paying customers.
- **Segment collapse**: Overall metric stable but one geographic region,
  channel, or device type is collapsing.

## Output format

Output ONLY a JSON array in a code block. Keep headlines SHORT
(under 15 words) and pattern-oriented (e.g., "Traffic growing but
revenue declining — vanity growth"). Use arrows in signal descriptions
(e.g., "Sessions ↑ 14%", "Revenue ↓ 8.5%").

```json
[
  {
    "headline": "Short pattern-oriented headline",
    "description": "2-3 sentences explaining what happened and why",
    "signals": [
      {
        "metric_id": "builtin:...",
        "description": "Sessions ↑ 14%",
        "value": 1500.0,
        "change": 0.14,
        "period_days": 7
      }
    ],
    "actions": [
      {
        "description": "Concrete next step, not generic advice",
        "priority": "high"
      }
    ],
    "counterfactual": {
      "value": 100.0,
      "metric_id": "builtin:...",
      "metric_name": "Metric Name",
      "description": "If X had held steady, Y would be Z higher",
      "revenue_impact": {
        "value": 5000.0,
        "description": "Recovery: ~$5k (7d)"
      }
    },
    "revenue_impact": {
      "value": 5000.0,
      "description": "Impact: -$5k (7d)"
    },
    "confidence": {
      "score": 0.75,
      "description": "Strong signal, segment identified"
    }
  }
]
```

## Guidelines

- Report drops > 5% AND contradictions between metrics.
- The `change` field is a decimal fraction (-0.15 = 15% drop).
- Priority: "high", "medium", or "low".
- If no significant issues, return `[]`.
- Actions must be SPECIFIC (name the page, flow, or channel to check),
  not generic ("investigate...", "review...", "monitor...").
- Do NOT report issues with 'unknown' or 'unattributed' segments.
"""

USER_MESSAGE = (
    "Analyze the dashboard metrics for significant changes in the "
    "last 7 days. Investigate any drops, find root causes, and "
    "produce insights."
)


# ---------------------------------------------------------------------------
# Tool functions
# ---------------------------------------------------------------------------


def list_dashboard_metrics() -> str:
    """List all dashboard metrics with their current values and recent
    changes. Metrics with a significant drop (> 5%) are flagged with [!]."""
    if _context is None:
        return "Error: no analysis context available."

    metrics = _context.dashboard_metrics
    if not metrics:
        return "No dashboard metrics found."

    lines: list[str] = ["Dashboard Metrics Overview", "=" * 50]
    for dm in metrics:
        snap = dm.snapshot
        if snap is None:
            lines.append(f"  {dm.metric_id}: no snapshot available")
            continue

        value = snap.value
        c7 = snap.change(TREND_WINDOW_DAYS, dm.aggregation)
        c3 = snap.change(3, dm.aggregation)
        c1 = snap.change(1, dm.aggregation)

        flag = " [!]" if c7 <= DROP_FLAG_THRESHOLD else ""
        lines.append(
            f"  {dm.metric_id}{flag}\n"
            f"    Value: {value:,.2f}  "
            f"(agg: {dm.aggregation.value})\n"
            f"    Change — 1d: {c1:+.1%}  "
            f"3d: {c3:+.1%}  7d: {c7:+.1%}"
        )

    return "\n".join(lines)


def get_metric_detail(metric_id: str) -> str:
    """Get detailed information about a specific metric including its
    values across multiple time windows and available dimensions."""
    if _context is None:
        return "Error: no analysis context available."

    dm = _context.get_dashboard_metric(metric_id)
    if dm is None:
        return f"Metric '{metric_id}' not found on the dashboard."

    snap = dm.snapshot
    metric = _context.get_metric(metric_id)

    lines: list[str] = [f"Metric Detail: {metric_id}", "=" * 50]

    if metric is not None:
        lines.append(f"  Name: {metric.name}")
        lines.append(f"  Description: {metric.description}")
        lines.append(f"  Data type: {metric.data_type.value}")
        if metric.dimensions:
            dim_list = ", ".join(
                f"{d.key} ({d.name})" for d in metric.dimensions
            )
            lines.append(f"  Dimensions: {dim_list}")
        else:
            lines.append("  Dimensions: none")
        if metric.table_keys:
            lines.append(
                f"  Tables: {', '.join(metric.table_keys)}"
            )

    if snap is None:
        lines.append("  Snapshot: not available")
        return "\n".join(lines)

    lines.append(f"  Aggregation: {dm.aggregation.value}")
    lines.append(f"  Current value: {snap.value:,.2f}")
    lines.append(f"  Data points: {len(snap.values)}")

    lines.append("  Time windows:")
    for days in [1, 3, 7, 14, 30]:
        agg_val = snap.aggregate_value(days, dm.aggregation)
        change = snap.change(days, dm.aggregation)
        lines.append(
            f"    {days:>2}d — aggregate: {agg_val:>12,.2f}  "
            f"change: {change:+.1%}"
        )

    return "\n".join(lines)


def breakdown_metric(
    metric_id: str, dimension: str, days: int = 7,
) -> str:
    """Break down a metric by a dimension to see which segments are
    driving a change. Shows each segment's recent vs previous values
    and percent change, sorted by largest absolute change."""
    if _context is None:
        return "Error: no analysis context available."

    metric = _context.get_metric(metric_id)
    if metric is None:
        return f"Metric '{metric_id}' not found."

    if not metric.dimensions:
        return f"Metric '{metric_id}' has no dimensions available."

    valid_dims = [d.key for d in metric.dimensions]
    if dimension not in valid_dims:
        return (
            f"Dimension '{dimension}' not available for "
            f"'{metric_id}'. Available: {', '.join(valid_dims)}"
        )

    dm = _context.get_dashboard_metric(metric_id)
    aggregation = dm.aggregation if dm else Aggregation.SUM

    try:
        series = metric.compute_series(
            _context._metric_context,
            dimensions=[dimension],
            aggregation=aggregation,
            days=days * 2,
            date=_context.as_of_date,
        )
    except Exception as e:
        return f"Error computing breakdown: {e}"

    midpoint = len(series.points) // 2
    recent: dict[str, float] = {}
    previous: dict[str, float] = {}

    for i, point in enumerate(series.points):
        for result in point.results:
            segment = (
                next(iter(result.groups.values()), "unknown")
                if result.groups
                else "unknown"
            )
            bucket = recent if i >= midpoint else previous
            bucket[segment] = bucket.get(segment, 0.0) + result.value

    segments: list[tuple[str, float, float, float]] = []
    all_keys = set(recent.keys()) | set(previous.keys())
    for seg in all_keys:
        r = recent.get(seg, 0.0)
        p = previous.get(seg, 0.0)
        pct = (r - p) / abs(p) if p != 0 else 0.0
        segments.append((seg, r, p, pct))

    segments.sort(key=lambda x: abs(x[2] - x[1]), reverse=True)
    segments = segments[:MAX_SEGMENTS]

    lines: list[str] = [
        f"Breakdown of {metric_id} by {dimension} "
        f"({days}d recent vs previous {days}d)",
        "=" * 60,
        f"  {'Segment':<25} {'Recent':>12} {'Previous':>12} "
        f"{'Change':>8}",
        "  " + "-" * 57,
    ]
    for seg, r, p, pct in segments:
        lines.append(
            f"  {seg:<25} {r:>12,.2f} {p:>12,.2f} "
            f"{pct:>+7.1%}"
        )

    return "\n".join(lines)


def list_tables() -> str:
    """List all available data tables and their column schemas."""
    if _context is None:
        return "Error: no analysis context available."

    descriptors = _context.list_tables()
    if not descriptors:
        return "No data tables available."

    lines: list[str] = ["Available Data Tables", "=" * 50]
    for desc in descriptors:
        lines.append(f"\n  Table: {desc.key}")
        lines.append(f"  Name: {desc.name}")
        if desc.description:
            lines.append(f"  Description: {desc.description}")
        lines.append("  Columns:")
        for col in desc.columns:
            pk = " [PK]" if col.is_primary_key else ""
            lines.append(
                f"    - {col.key} ({col.type.value}{pk})"
                f"{': ' + col.description if col.description else ''}"
            )

    return "\n".join(lines)


def query_table(
    table_key: str,
    group_by: str = "",
    aggregate_field: str = "",
    aggregate_method: str = "sum",
    filter_field: str = "",
    filter_operator: str = "eq",
    filter_value: str = "",
    limit: int = 20,
) -> str:
    """Query a data table with optional filtering, grouping, and
    aggregation.

    Args:
        table_key: The table to query (from list_tables).
        group_by: Column to group by (optional).
        aggregate_field: Column to aggregate (optional).
        aggregate_method: One of: sum, avg, min, max, count.
        filter_field: Column to filter on (optional).
        filter_operator: One of: eq, neq, gt, gte, lt, lte, contains.
        filter_value: Value to filter by (optional).
        limit: Max rows to return (default 20, max 50).
    """
    if _context is None:
        return "Error: no analysis context available."

    try:
        table = _context.get_table(table_key)
    except (ValueError, KeyError) as e:
        return f"Error: table '{table_key}' not found — {e}"

    if filter_field and filter_value:
        try:
            op = FilterOperator(filter_operator)
        except ValueError:
            return (
                f"Invalid filter operator '{filter_operator}'. "
                f"Use: eq, neq, gt, gte, lt, lte, contains."
            )
        table = table.filter(filter_field, op, filter_value)

    if group_by and aggregate_field:
        try:
            method = Aggregation(aggregate_method)
        except ValueError:
            return (
                f"Invalid aggregation '{aggregate_method}'. "
                f"Use: sum, avg, min, max, count."
            )
        grouped = table.group_by(group_by)
        agg_result = grouped.aggregate(aggregate_field, method)

        items = sorted(
            agg_result.items(), key=lambda x: x[1], reverse=True,
        )
        items = items[:MAX_TABLE_ROWS]

        lines: list[str] = [
            f"Grouped by {group_by}, "
            f"{aggregate_method}({aggregate_field})",
            "=" * 40,
        ]
        for key, val in items:
            lines.append(f"  {key:<30} {val:>12,.2f}")
        lines.append(f"\n  ({len(agg_result)} groups total)")
        return "\n".join(lines)

    if aggregate_field and not group_by:
        try:
            method = Aggregation(aggregate_method)
        except ValueError:
            return (
                f"Invalid aggregation '{aggregate_method}'. "
                f"Use: sum, avg, min, max, count."
            )
        val = table.aggregate(aggregate_field, method)
        return (
            f"{aggregate_method}({aggregate_field}) = {val:,.2f}"
        )

    capped = min(limit, MAX_TABLE_ROWS)
    table = table.limit(capped)
    rows = table.rows()

    if not rows:
        return "Query returned no rows."

    columns = [col.key for col in table.descriptor().columns]
    lines = [
        f"Table: {table_key} ({len(rows)} rows)",
        "=" * 50,
    ]

    header = "  ".join(f"{c:<20}" for c in columns[:8])
    lines.append(header)
    lines.append("-" * len(header))

    for row in rows:
        vals: list[str] = []
        for c in columns[:8]:
            v = row.get(c)
            if isinstance(v, float):
                vals.append(f"{v:<20,.2f}")
            else:
                s = str(v) if v is not None else ""
                vals.append(f"{s:<20}"[:20])
        lines.append("  ".join(vals))

    return "\n".join(lines)


def estimate_revenue_impact(metric_id: str, days: int = 7) -> str:
    """Estimate the revenue impact of a metric's change over a given
    number of days."""
    if _context is None:
        return "Error: no analysis context available."

    rev_dm = _context.get_dashboard_metric(GROSS_REVENUE_METRIC_ID)
    if rev_dm is None or rev_dm.snapshot is None:
        return "No revenue data available on the dashboard."

    rev_snap = rev_dm.snapshot
    rev_recent = rev_snap.aggregate_value(days, rev_dm.aggregation)
    rev_previous = (
        rev_snap.aggregate_value(days * 2, rev_dm.aggregation)
        - rev_recent
    )
    rev_change = rev_snap.change(days, rev_dm.aggregation)
    rev_lost = max(rev_previous - rev_recent, 0.0) / 100

    target_dm = _context.get_dashboard_metric(metric_id)
    target_info = ""
    if target_dm and target_dm.snapshot:
        t_change = target_dm.snapshot.change(
            days, target_dm.aggregation,
        )
        target_info = (
            f"  {metric_id} changed by {t_change:+.1%} "
            f"over the same period.\n"
        )

    lines: list[str] = [
        f"Revenue Impact Estimate ({days}d)",
        "=" * 40,
        f"  Revenue (recent {days}d): "
        f"${rev_recent / 100:,.2f}",
        f"  Revenue (previous {days}d): "
        f"${rev_previous / 100:,.2f}",
        f"  Revenue change: {rev_change:+.1%}",
    ]

    if rev_lost > 0:
        lines.append(f"  Estimated revenue lost: ${rev_lost:,.2f}")
    else:
        lines.append("  No revenue loss detected.")

    if target_info:
        lines.append(target_info)

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Output parsing
# ---------------------------------------------------------------------------

_PRIORITY_MAP: dict[str, Priority] = {
    "high": Priority.HIGH,
    "medium": Priority.MEDIUM,
    "low": Priority.LOW,
}


def _parse_insights(text: str) -> list[Insight]:
    json_str = _extract_json(text)
    if json_str is None:
        return []

    try:
        raw = json.loads(json_str)
    except (json.JSONDecodeError, ValueError):
        logger.warning("Failed to parse agent JSON output")
        return []

    if not isinstance(raw, list):
        return []

    insights: list[Insight] = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        try:
            insight = _build_insight(item)
            insights.append(insight)
        except Exception:
            logger.debug(
                "Skipping malformed insight item", exc_info=True,
            )
    return insights


def _extract_json(text: str) -> str | None:
    match = re.search(
        r"```(?:json)?\s*\n?(.*?)```",
        text,
        re.DOTALL,
    )
    if match:
        return match.group(1).strip()

    match = re.search(r"\[.*\]", text, re.DOTALL)
    if match:
        return match.group(0)

    return None


def _build_insight(item: dict) -> Insight:
    signals = [
        Signal(
            metric_id=s.get("metric_id", ""),
            description=s.get("description", ""),
            value=float(s.get("value", 0.0)),
            change=float(s.get("change", 0.0)),
            period_days=int(s.get("period_days", 0)),
        )
        for s in item.get("signals", [])
        if isinstance(s, dict)
    ]

    actions = [
        Action(
            description=a.get("description", ""),
            priority=_PRIORITY_MAP.get(
                a.get("priority", "medium"), Priority.MEDIUM,
            ),
        )
        for a in item.get("actions", [])
        if isinstance(a, dict)
    ]

    ri_raw = item.get("revenue_impact", {}) or {}
    revenue_impact = RevenueImpact(
        value=float(ri_raw.get("value", 0.0)),
        description=ri_raw.get("description", ""),
    )

    cf_raw = item.get("counterfactual", {}) or {}
    cf_ri_raw = cf_raw.get("revenue_impact", {}) or {}
    counterfactual = Counterfactual(
        value=float(cf_raw.get("value", 0.0)),
        metric_id=cf_raw.get("metric_id", ""),
        metric_name=cf_raw.get("metric_name", ""),
        description=cf_raw.get("description", ""),
        revenue_impact=RevenueImpact(
            value=float(cf_ri_raw.get("value", 0.0)),
            description=cf_ri_raw.get("description", ""),
        ),
    )

    conf_raw = item.get("confidence", {}) or {}
    confidence = Confidence(
        score=max(0.0, min(1.0, float(conf_raw.get("score", 0.0)))),
        description=conf_raw.get("description", ""),
    )

    return Insight(
        id=f"agentic-{uuid.uuid4().hex[:8]}",
        headline=item.get("headline", ""),
        description=item.get("description", ""),
        signals=signals,
        actions=actions,
        counterfactual=counterfactual,
        revenue_impact=revenue_impact,
        confidence=confidence,
        source=InsightSource.AGENTIC,
    )


# ---------------------------------------------------------------------------
# Analyzer
# ---------------------------------------------------------------------------

_TOOLS = [
    list_dashboard_metrics,
    get_metric_detail,
    breakdown_metric,
    list_tables,
    query_table,
    estimate_revenue_impact,
]


class AgenticAnalyzer(Analyzer):

    def analyze(self, ctx: AnalysisContext) -> AnalysisResult:
        global _context
        _context = ctx
        try:
            return self._run_agent()
        except Exception:
            logger.error(
                "Agentic analyzer failed", exc_info=True,
            )
            return AnalysisResult()
        finally:
            _context = None

    def _run_agent(self) -> AnalysisResult:
        agent = Agent(
            name="kpi_analyzer",
            model=make_model(),
            instruction=AGENT_INSTRUCTION,
            description=(
                "Investigates KPI metric drops and produces "
                "root-cause insights."
            ),
            tools=_TOOLS,
        )

        final_text = run_adk_agent(agent, USER_MESSAGE, max_turns=10)
        insights = _parse_insights(final_text)
        return AnalysisResult(
            insights=insights,
            analyzed_at=datetime.now(timezone.utc),
        )
