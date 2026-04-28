# Data Layer

The data layer (`kpidebug/data/`) provides a pluggable abstraction over external data sources, exposing them as queryable tables.

## DataTable interface

`DataTable` (`data/table.py`) is the central abstraction. All operations return new `DataTable` instances (immutable builder pattern).

```
DataTable
├── descriptor() -> TableDescriptor
├── filter(field, operator, value) -> DataTable       # chainable
├── select(*columns) -> DataTable
├── sort(field, ascending) -> DataTable
├── limit(count, offset) -> DataTable
├── group_by(*fields) -> GroupedTable
├── aggregate(field, method: Aggregation) -> float
├── join(other, on) -> DataTable
├── union(other) -> DataTable
├── query(TableQuery) -> DataTable                    # applies filters + sort + limit from a query object
├── filter_rows(predicate) -> DataTable               # lambda-based, materializes to InMemoryDataTable
└── add_column(key, name, compute_fn) -> DataTable    # computed column, materializes to InMemoryDataTable
```

### Implementations

- **`InMemoryDataTable`** (`table_memory.py`): Holds `list[Row]` in memory. All operations create new instances with filtered/transformed row lists.
- **`PostgresDataTable`** (`table_postgres.py`): Lazy SQL query builder. Accumulates WHERE/ORDER/LIMIT clauses, executes on materialization (`rows()`, `count()`, `aggregate()`).

### Supporting types (`data/types.py`)

- `Row = dict[str, RowValue]` where `RowValue = str | int | float | bool | None`
- `TableDescriptor`: schema with key, name, columns list
- `TableColumn`: key, name, type (`ColumnType` enum), is_primary_key, annotations
- `TableFilter`, `TableQuery`, `TableResult`: query/response types
- `FilterOperator`: EQ, NEQ, CONTAINS, GT, GTE, LT, LTE
- `Aggregation`: SUM, AVG, MIN, MAX, COUNT, AVG_DAILY

## Connectors

`DataSourceConnector` (`connector.py`) is the abstract interface for external data source integrations.

```
DataSourceConnector
├── validate_credentials() -> bool
├── get_tables() -> list[TableDescriptor]
├── fetch_table_data(table_key, query) -> TableResult
├── fetch_all_rows(table_key) -> list[Row]
└── fetch_table(table_key) -> DataTable               # convenience: wraps fetch_all_rows in InMemoryDataTable
```

### Implementations

- **Stripe** (`data/stripe/connector.py`): Paginated Stripe API calls. Tables: `stripe:charges`, `stripe:customers`, `stripe:subscriptions`, `stripe:payouts`, `stripe:refunds`, `stripe:disputes`, `stripe:invoices`, `stripe:balance_transactions`.
- **Google Analytics** (`data/google_analytics/connector.py`): GA Data API via `google-analytics-data` SDK. Tables: `google_analytics:traffic_sources`, `google_analytics:pages`, etc.

### CachedConnector (`cached_connector.py`)

Wraps a live connector + data source store. Caches fetched rows in PostgreSQL (`cached_table_data` table) so metrics can be computed without hitting external APIs on every request.

- `sync_table(table_key)`: Fetches from live connector, persists to cache
- `sync_all()`: Syncs all tables, returns `SyncAllResult` with row counts and errors
- `fetch_table()` / `fetch_all_rows()`: Reads from cache first, falls back to live

## Data source store

`AbstractDataSourceStore` / `PostgresDataSourceStore` manages data source CRUD and the row cache.

Tables: `data_sources` (credentials stored as JSON), `cached_table_data` (source_id, table_key, rows as JSONB, synced_at).
