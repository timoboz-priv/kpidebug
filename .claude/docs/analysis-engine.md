# Analysis Engine

The analysis engine (`kpidebug/analysis/`) detects actionable insights by running templated scenarios over dashboard metrics and their underlying data.

## Core concepts

| Concept | File | Purpose |
|---------|------|---------|
| `AnalysisContext` | `analysis/context.py` | Holds dashboard metrics, lazily resolves `Metric` objects and `DataTable`s, optional `as_of_date` |
| `Analyzer` | `analysis/analyzer.py` | Abstract base: `analyze(ctx) -> AnalysisResult` |
| `TemplateAnalyzer` | `analysis/analyzer_template.py` | Runs a list of `InsightTemplate`s, collects non-None results |
| `InsightTemplate` | `analysis/analyzer_template.py` | Abstract base: `evaluate(ctx) -> Insight | None` |

## Result types (`analysis/types.py`)

```
AnalysisResult
├── insights: list[Insight]
│   ├── headline: str
│   ├── description: str
│   ├── signals: list[Signal]
│   │   ├── metric_id, description, value, change, period_days
│   ├── actions: list[Action]
│   │   ├── description, priority: Priority (HIGH/MEDIUM/LOW)
│   └── upside_potential: UpsidePotential
│       ├── value, metric_id, metric_name, description
└── analyzed_at: datetime
```

## AnalysisContext

Wraps a `MetricContext` and the dashboard's pinned metrics. All loading is lazy-then-cached:

- `dashboard_metrics` — direct access to the pre-loaded `DashboardMetric` list (snapshots included)
- `get_dashboard_metric(metric_id)` — lookup by metric id
- `get_metric(metric_id)` — lazy: resolves via builtin registry then metric store, caches
- `list_metrics()` — resolves all dashboard metrics
- `get_table(table_key)` — lazy: delegates to `MetricContext.table()`, caches
- `list_tables()` — collects `TableDescriptor`s from all connectors
- `as_of_date` — optional, set by `process_simulate` for historical analysis

## Thresholds and utilities (`analysis/utils.py`)

Constants for change classification:

| Constant | Value | Meaning |
|----------|-------|---------|
| `LARGE_DROP_THRESHOLD` | -0.15 | -15% or worse |
| `SMALL_DROP_THRESHOLD` | -0.05 | -5% to -15% |
| `NEGLIGIBLE_THRESHOLD` | 0.02 | within ±2%, noise |
| `SMALL_GAIN_THRESHOLD` | 0.05 | +5% to +15% |
| `LARGE_GAIN_THRESHOLD` | 0.15 | +15% or better |
| `MIN_DATA_POINTS` | 7 | Minimum days of data to analyze |
| `TREND_WINDOW_DAYS` | 7 | Default comparison window |
| `COMPARISON_WINDOW_DAYS` | 30 | Longer comparison window |

`classify_change(change: float) -> ChangeCategory` maps a percentage change to one of: `LARGE_DROP`, `SMALL_DROP`, `NEGLIGIBLE`, `SMALL_GAIN`, `LARGE_GAIN`.

## Templates

### AcquisitionDropTemplate (`templates/acquisition_drop.py`)

**Fires when**: Sessions show a significant drop but conversion rate is stable (within `NEGLIGIBLE_THRESHOLD`).

**Extra signal**: Resolves the sessions metric and computes a channel breakdown via `session_channel_group` dimension. If any single channel dropped >30%, flags it as the root cause.

**Uses**: `builtin:ga.sessions`, `builtin:ga.conversion_rate`

### ConversionBreakdownTemplate (`templates/conversion_breakdown.py`)

**Fires when**: Conversion rate drops significantly but traffic is stable.

**Corroborating signals**: Checks `builtin:ga.conversions`, `builtin:stripe.gross_revenue`, `builtin:stripe.customer_count` for aligned drops.

**Funnel analysis**: Two paths:
1. When `builtin:ga.signup_rate` and `builtin:ga.signup_to_paid_rate` are pinned: compares both step rates, identifies the weakest step
2. Fallback: compares absolute conversions (GA) vs new customers (Stripe) as an approximation

## Processor (`kpidebug/processor.py`)

Orchestrates data sync, metrics computation, and analysis. Lives at the package root because it sits above both the metrics and analysis layers.

### `process_all`

Production entry point. Syncs data, computes and stores metric snapshots, runs analysis.

```python
def process_all(
    project_id, data_source_store, dashboard_store, metric_store,
    mode: ProcessMode = ProcessMode.FULL,
) -> None
```

`ProcessMode` controls which steps run:
- `FULL` — sync + metrics + analysis
- `METRICS` — metrics + analysis (skip sync, use cached data)
- `ANALYSIS` — analysis only (use existing snapshots)

### `process_simulate`

Testing/backtesting entry point. Computes metrics for a historical date without writing to the database.

```python
def process_simulate(
    project_id, data_source_store, dashboard_store, metric_store,
    as_of_date: date | None = None,
) -> AnalysisResult
```

- Never syncs data sources
- Computes `metric.compute_series(days=60, date=as_of_date)` for each pinned metric
- Builds in-memory `DashboardMetric` copies with simulated snapshots (no DB writes)
- Passes `as_of_date` into `AnalysisContext` for downstream visibility
- Returns the `AnalysisResult` directly
