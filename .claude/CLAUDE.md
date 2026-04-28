# The project

KPI debugger, a reasoning engine on top of your metrics. It doesn't just show you analytics or 
identifies anomalies, it actively finds issues, correlates them across different data sources, finds root
causes, and tells you what to fix.

## Common infrastructure
- Users can login using username/password or Google (using Firebase)
- Projects are the main entity that users work on. Users can be assigned to projects with different access rights:
  - Read: Can see all metrics, analytics, advice
  - Edit: Can create new metrics etc.
  - Admin: Can delete projects, connect data sources, ...


# Architecture

## Structure
```
kpidebug/                        # Backend application package
├── api/                         # FastAPI routes and middleware
│   ├── server.py                # App init, CORS, routers, exception handler
│   ├── auth.py                  # Firebase token verification, role-based access
│   ├── stores.py                # DI: factory functions returning store instances
│   ├── routes_users.py          # GET /api/users/me
│   ├── routes_projects.py       # /api/projects CRUD + membership
│   ├── routes_metrics.py        # /api/projects/{id}/metrics CRUD + computation
│   ├── routes_dashboard.py      # /api/projects/{id}/dashboard pinned metrics
│   ├── routes_data_sources.py   # /api/projects/{id}/data-sources management
│   └── routes_data_tables.py    # /api/projects/{id}/data-tables queries
├── processor.py                 # Orchestrates sync, metrics, analysis (see below)
├── analysis/                    # Analysis engine (see docs/analysis-engine.md)
│   ├── types.py                 # Insight, Signal, Action, UpsidePotential, Priority
│   ├── context.py               # AnalysisContext: lazy metric/table resolution
│   ├── utils.py                 # ChangeCategory, classify_change, thresholds
│   ├── analyzer.py              # Abstract Analyzer base class
│   ├── analyzer_template.py     # TemplateAnalyzer + InsightTemplate
│   └── templates/               # Concrete insight templates
│       ├── acquisition_drop.py  # Traffic drop with stable conversion
│       └── conversion_breakdown.py  # Conversion drop with stable traffic
├── metrics/                     # Metrics engine (see docs/metrics-engine.md)
│   ├── types.py                 # Metric, MetricDefinition, MetricSnapshot, enums
│   ├── computation.py           # DSL tokenizer, parser, evaluator
│   ├── context.py               # MetricContext: bridges DSL to data sources
│   ├── expression_metric.py     # ExpressionMetric: user-defined metric wrapper
│   ├── registry.py              # Builtin metric registry
│   ├── metric_store.py          # AbstractMetricStore interface
│   ├── metric_store_postgres.py # PostgreSQL implementation
│   ├── dashboard_store.py       # AbstractDashboardStore interface
│   ├── dashboard_store_postgres.py
│   ├── stripe/metrics.py        # Builtin Stripe metrics (auto-registered)
│   └── google_analytics/metrics.py  # Builtin GA metrics (auto-registered)
├── data/                        # Data layer (see docs/data-layer.md)
│   ├── types.py                 # DataSource, TableDescriptor, Row, enums
│   ├── table.py                 # DataTable abstract interface + GroupedTable
│   ├── table_memory.py          # InMemoryDataTable implementation
│   ├── table_postgres.py        # PostgresDataTable (lazy SQL builder)
│   ├── connector.py             # DataSourceConnector abstract interface
│   ├── cached_connector.py      # Caching wrapper around live connectors
│   ├── data_source_store.py     # AbstractDataSourceStore interface
│   ├── data_source_store_postgres.py
│   ├── stripe/connector.py      # Stripe API connector
│   └── google_analytics/connector.py  # GA Data API connector
├── management/                  # User & project management
│   ├── types.py                 # User, Project, ProjectMember, Role, ArtifactType
│   ├── user_store.py            # AbstractUserStore
│   ├── user_store_postgres.py
│   ├── project_store.py         # AbstractProjectStore
│   ├── project_store_postgres.py
│   ├── artifact_store.py        # AbstractArtifactStore (files/URLs)
│   ├── artifact_store_postgres.py
│   └── summary_agent.py         # Claude-powered project summaries
├── common/                      # Shared utilities
│   ├── db.py                    # ConnectionPoolManager (psycopg3 pool singleton)
│   ├── logging.py               # Centralized logging setup
│   └── math.py                  # aggregate_values() helper
└── config.py                    # Config from environment (.env)

ui/                              # React TypeScript frontend
├── src/
│   ├── App.tsx                  # Router + protected routes
│   ├── theme.ts                 # MUI theme
│   ├── firebase.ts              # Firebase client init
│   ├── api/
│   │   ├── client.ts            # Axios instance with Bearer token interceptor
│   │   ├── projects.ts
│   │   ├── dataSources.ts
│   │   └── users.ts
│   ├── contexts/
│   │   ├── UserContext.tsx       # Firebase auth state
│   │   └── ProjectContext.tsx    # Project selection (persisted to localStorage)
│   ├── layout/
│   │   ├── AppLayout.tsx
│   │   └── Sidebar.tsx
│   ├── pages/
│   │   ├── LoginPage.tsx
│   │   ├── MetricsDashboardPage.tsx  # Default route (/)
│   │   ├── MetricsPage.tsx          # /metrics/explorer
│   │   ├── DataTablesPage.tsx       # /data
│   │   └── ProjectSettingsPage.tsx  # /settings
│   └── components/              # Reusable UI components

tests/                           # Mirrors kpidebug/ structure
scripts/                         # Database setup scripts
```

## Backend
- **Framework**: FastAPI + Uvicorn
- **Database**: PostgreSQL via psycopg3 (raw SQL, no ORM). Connection pool singleton in `common/db.py`
- **Auth**: Firebase ID tokens verified server-side. Role hierarchy: READ < EDIT < ADMIN
- **Dependency injection**: FastAPI `Depends()` with store factory functions in `api/stores.py`
- **Python**: 3.13. Virtual environment in `.venv`, dependencies in `requirements.txt`
- **Environment**: `.env` file loaded via python-dotenv. Key vars: `DATABASE_URL`, `GOOGLE_APPLICATION_CREDENTIALS`, `FRONTEND_URL`

## Frontend
- **Stack**: React 19, TypeScript, MUI 9, React Router 7, Axios, Recharts
- **Auth**: Firebase client SDK. Token injected into Axios interceptor
- **State**: React Context for user auth + project selection. No external state library
- **API communication**: REST via Axios client. The API client wraps typed calls to backend endpoints

## Key subsystems
- [Metrics Engine](docs/metrics-engine.md) - DSL-based metric computation, builtin metrics, snapshots
- [Data Layer](docs/data-layer.md) - DataTable abstraction, connectors, caching
- [Analysis Engine](docs/analysis-engine.md) - Template-based insight detection, AnalysisContext, processor orchestration


# Patterns

## Store pattern
All persistence uses the abstract store pattern:
- Abstract base class defines the interface (e.g. `AbstractMetricStore`)
- PostgreSQL implementation in a separate file (e.g. `PostgresMetricStore`)
- Stores are instantiated via factory functions in `api/stores.py` and injected with `Depends()`
- Stores receive a `ConnectionPoolManager` instance, not raw connection strings

## DataTable as query builder
`DataTable` operations (`filter`, `select`, `sort`, `limit`) return new instances (immutable builder). `InMemoryDataTable` materializes eagerly, `PostgresDataTable` accumulates SQL clauses and executes lazily.

## Metric types
- **Builtin metrics**: Concrete `Metric` subclasses (e.g. `GrossRevenueMetric`) that implement `compute_single()` directly. Auto-registered on module import via `registry.register()`
- **User-defined metrics**: `MetricDefinition` with a DSL `computation` string, wrapped in `ExpressionMetric` at runtime

## Data source connectors
Each integration (Stripe, GA) implements `DataSourceConnector`. At runtime, connectors are wrapped in `CachedConnector` which syncs data to PostgreSQL cache before metrics computation.


# Style

## Backend Code
- In general, stick to the conventions you already find in existing code
- Always use types. Don't use untyped arguments and avoid Any if possible. Don't return dicts but proper types
- Declare properties of types explicitly, don't just assign them in constructors
- Avoid circular imports. Alert if one is about to be introduced, don't just blindly solve with TYPE_CHECKING
- Use dataclasses and dataclasses-json for schema and data model classes. Decorator order: `@dataclass_json` then `@dataclass`
- Use `self` not `cls` for class methods. Don't use the @classmethod annotations
- Prefer `__init__` to `new`
- Enums inherit from `(str, Enum)` for JSON serialization
- Use enums for fields with a fixed set of distinct values (priorities, statuses, categories), not plain strings
- Raw SQL with psycopg3 parameter binding. No ORM, no query builder
- Logging via stdlib `logging.getLogger(__name__)`. No print statements

## Backend Testing
- Write tests for all major functionality. Test a common path and a few edge cases
- Follow the code structure when writing tests, i.e. one test module/file for each code module/file. One test class for each code class
- Run and make sure the tests pass
- Test framework: pytest. Mocking: `unittest.mock`
- Store tests use real PostgreSQL via `ConnectionPool` fixtures with `ensure_tables()` / `drop_tables()` lifecycle

## Frontend code
- MUI components for all UI elements
- Typed API calls via wrapper functions in `ui/src/api/`
- React Context for cross-cutting state (auth, project selection)

## Frontend testing
- No need for frontend tests
