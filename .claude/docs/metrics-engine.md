# Metrics Engine

The metrics engine (`kpidebug/metrics/`) is a DSL-based computation system that defines and evaluates KPIs over pluggable data sources.

## Core concepts

| Concept | Type | File |
|---------|------|------|
| `Metric` | Abstract base class | `metrics/types.py` |
| `ExpressionMetric` | User-defined metric backed by a DSL computation string | `metrics/expression_metric.py` |
| `MetricDefinition` | Persisted definition with DSL `computation` field | `metrics/types.py` |
| `MetricContext` | Bridges DSL evaluation to live data sources | `metrics/context.py` |
| `MetricSnapshot` | Pre-computed time series stored on dashboard metrics | `metrics/types.py` |
| Registry | Global dict of builtin `Metric` instances, keyed by id | `metrics/registry.py` |
| Processor | Batch sync + compute loop for dashboard snapshots | `metrics/processor.py` |

## Metric resolution

Metrics are resolved by id in `processor._resolve_metric()`:
1. Check the builtin registry (`registry.get(metric_id)`)
2. Fall back to a user-defined `MetricDefinition` from the metric store
3. Wrap definitions in `ExpressionMetric` to satisfy the `Metric` interface

Builtin metric ids are namespaced: `builtin:stripe.gross_revenue`, `builtin:ga.sessions`.

## DSL grammar

The computation DSL lives in `metrics/computation.py`. It is a SQL-like expression language for aggregating table data.

```
expression     := ratio_expr | arithmetic_expr
arithmetic_expr := primary (('+' | '-' | '*' | '/') primary)*
primary        := aggregate | '(' expression ')' | NUMBER
aggregate      := AGG_FN '(' field? ')' ['from' table_name] [where_clause]
ratio_expr     := 'ratio' '(' expression ',' expression ')'
where_clause   := 'where' condition ('and' condition)*
condition      := field COMP_OP value
AGG_FN         := 'sum' | 'count' | 'avg' | 'min' | 'max'
COMP_OP        := '=' | '!=' | '>' | '>=' | '<' | '<=' | 'contains'
```

### Examples

```
sum(amount) from charges
count() from customers
avg(amount) from charges where status = "paid"
ratio(count() from charges where status = "succeeded", count() from charges)
sum(amount) from charges where status = "paid" and amount > "100"
sum(amount) / count() * 100
```

### Pipeline

1. **Tokenizer** (`_tokenize`): Lexes into `_Token(type, value)` list
2. **Parser** (`_Parser`): Recursive descent, builds AST of `_AggNode`, `_LiteralNode`, `_ArithNode`, `_RatioNode`
3. **Evaluator** (`_eval_node`): Walks the AST, resolves tables from `MetricContext`, applies filters, runs aggregation

### Entry points

- `validate(expression)` - parse only, raises `ComputationError` on syntax errors
- `evaluate(expression, rows)` - parse + evaluate against in-memory row list (no table resolution)
- `evaluate_with_context(expression, ctx)` - parse + evaluate with live `MetricContext` (table `from` clauses resolved via connectors)

### Table resolution

- With `from table_name`: Resolves via `MetricContext.table(table_name)`, which searches all connected data source connectors
- Without `from`: Uses a default table passed to `_eval_node` (only works with `evaluate()`, not `evaluate_with_context()`)

## Builtin metrics

Builtin metrics are concrete `Metric` subclasses that implement `compute_single()` directly rather than through the DSL.

### Stripe (`metrics/stripe/metrics.py`)
Auto-registered on import. Table keys prefixed with `stripe:` (charges, customers, subscriptions, payouts, refunds, disputes, invoices, balance_transactions).

Key metrics: `GrossRevenueMetric`, `NetRevenueMetric`, `MrrMetric`, `CustomerCountMetric`, `RefundRateMetric`.

### Google Analytics (`metrics/google_analytics/metrics.py`)
Auto-registered on import. Table keys prefixed with `google_analytics:` (traffic_sources, pages, events, geography, devices).

Uses a `_make_sum()` factory for simple aggregation metrics and custom implementations for rate metrics (weighted averages).

## Time series computation

`Metric.compute_series()` (in `types.py`) iterates over a date range by `TimeBucket` (DAY, WEEK, MONTH), calling `compute_single()` for each bucket. The helper `apply_time_filter(table, time_column, days, date)` applies ISO 8601 range filters to a `DataTable`.

## Snapshot lifecycle

1. `processor.process_all(project_id, ...)` is the entry point for batch computation
2. Syncs all data sources via `CachedConnector.sync_all()`
3. Builds a `MetricContext` for the project
4. For each pinned `DashboardMetric`: resolves the metric, calls `compute_series(days=60)`, stores the resulting `MetricSnapshot`
5. Dashboard API routes read snapshots to return sparkline data and aggregated values (1d, 3d, 7d, 30d)

## Stores

Metrics persistence follows the abstract store pattern used throughout the codebase:

- `AbstractMetricStore` / `PostgresMetricStore` - metric definitions and computed results
- `AbstractDashboardStore` / `PostgresDashboardStore` - pinned dashboard metrics and snapshots

Tables: `metric_definitions`, `metric_results`, `dashboard_metrics`.
