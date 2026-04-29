"""Set up a test project with synthetic data.

Creates a project with a fixed ID, loads CSV test data from
testdata/startup_demo/ as cached connector data, and pins
dashboard metrics.

Usage:
    python scripts/startup_demo/setup_testproject.py
"""

import csv
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent.parent / ".env")

from kpidebug.common.logging import init_logging
init_logging()

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.user_store_postgres import PostgresUserStore
from kpidebug.management.project_store_postgres import PostgresProjectStore
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.metrics.dashboard_store_postgres import PostgresDashboardStore
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore
from kpidebug.data.types import Aggregation, DataSourceType

PROJECT_ID = "test-startup-demo"
PROJECT_NAME = "Startup Demo"
PROJECT_DESC = "Synthetic test project simulating a $10M ARR SaaS startup"

USER_EMAIL = "timo.boz@gmail.com"

DATA_DIR = Path(__file__).resolve().parent.parent.parent / "testdata" / "startup_demo"

STRIPE_SOURCE_ID = "src-stripe-demo"
GA_SOURCE_ID = "src-ga-demo"

CSV_TO_TABLE = {
    "stripe_charges.csv": ("stripe:charges", STRIPE_SOURCE_ID),
    "stripe_customers.csv": ("stripe:customers", STRIPE_SOURCE_ID),
    "stripe_subscriptions.csv": ("stripe:subscriptions", STRIPE_SOURCE_ID),
    "stripe_invoices.csv": ("stripe:invoices", STRIPE_SOURCE_ID),
    "stripe_refunds.csv": ("stripe:refunds", STRIPE_SOURCE_ID),
    "stripe_balance_transactions.csv": ("stripe:balance_transactions", STRIPE_SOURCE_ID),
    "stripe_disputes.csv": ("stripe:disputes", STRIPE_SOURCE_ID),
    "stripe_products.csv": ("stripe:products", STRIPE_SOURCE_ID),
    "stripe_prices.csv": ("stripe:prices", STRIPE_SOURCE_ID),
    "stripe_payouts.csv": ("stripe:payouts", STRIPE_SOURCE_ID),
    "ga_sessions_daily.csv": ("google_analytics:sessions_daily", GA_SOURCE_ID),
    "ga_traffic_sources.csv": ("google_analytics:traffic_sources", GA_SOURCE_ID),
    "ga_pages.csv": ("google_analytics:pages", GA_SOURCE_ID),
    "ga_landing_pages.csv": ("google_analytics:landing_pages", GA_SOURCE_ID),
    "ga_events.csv": ("google_analytics:events", GA_SOURCE_ID),
    "ga_geography.csv": ("google_analytics:geography", GA_SOURCE_ID),
    "ga_devices.csv": ("google_analytics:devices", GA_SOURCE_ID),
    "ga_users.csv": ("google_analytics:users", GA_SOURCE_ID),
    "ga_user_acquisition.csv": ("google_analytics:user_acquisition", GA_SOURCE_ID),
    "ga_conversions.csv": ("google_analytics:conversions", GA_SOURCE_ID),
    "ga_ecommerce.csv": ("google_analytics:ecommerce", GA_SOURCE_ID),
}

DASHBOARD_METRICS = [
    ("builtin:stripe.gross_revenue", Aggregation.SUM),
    ("builtin:stripe.net_revenue", Aggregation.SUM),
    ("builtin:stripe.mrr", Aggregation.SUM),
    ("builtin:stripe.customer_count", Aggregation.SUM),
    ("builtin:stripe.refund_rate", Aggregation.AVG_DAILY),
    ("builtin:stripe.invoice_collection_rate", Aggregation.AVG_DAILY),
    ("builtin:stripe.failed_payment_count", Aggregation.SUM),
    ("builtin:stripe.churn_count", Aggregation.SUM),
    ("builtin:stripe.retention_rate", Aggregation.AVG_DAILY),
    ("builtin:stripe.retention_30d", Aggregation.AVG_DAILY),
    ("builtin:ga.sessions", Aggregation.SUM),
    ("builtin:ga.total_users", Aggregation.SUM),
    ("builtin:ga.new_users", Aggregation.SUM),
    ("builtin:ga.page_views", Aggregation.SUM),
    ("builtin:ga.conversion_rate", Aggregation.AVG_DAILY),
    ("builtin:ga.conversions", Aggregation.SUM),
    ("builtin:ga.bounce_rate", Aggregation.AVG_DAILY),
    ("builtin:ga.engagement_rate", Aggregation.AVG_DAILY),
    ("builtin:ga.avg_session_duration", Aggregation.AVG_DAILY),
    ("builtin:ga.revenue_per_conversion", Aggregation.AVG_DAILY),
    ("builtin:ga.signup_rate", Aggregation.AVG_DAILY),
    ("builtin:ga.signup_to_paid_rate", Aggregation.AVG_DAILY),
]


def coerce_value(value: str) -> str | int | float | bool | None:
    if value == "" or value == "None":
        return None
    if value == "True":
        return True
    if value == "False":
        return False
    try:
        int_val = int(value)
        if str(int_val) == value:
            return int_val
    except ValueError:
        pass
    try:
        return float(value)
    except ValueError:
        pass
    return value


def load_csv(filepath: Path) -> list[dict]:
    rows = []
    with open(filepath, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append({k: coerce_value(v) for k, v in row.items()})
    return rows


def cleanup_project(pool_manager: ConnectionPoolManager) -> None:
    pool = pool_manager.pool()
    with pool.connection() as conn:
        source_ids = conn.execute(
            "SELECT id FROM data_sources WHERE project_id = %s",
            (PROJECT_ID,),
        ).fetchall()

        for (sid,) in source_ids:
            conn.execute("DELETE FROM cached_table_data WHERE source_id = %s", (sid,))
            conn.execute("DELETE FROM cached_table_meta WHERE source_id = %s", (sid,))

        conn.execute("DELETE FROM data_sources WHERE project_id = %s", (PROJECT_ID,))
        conn.execute("DELETE FROM dashboard_metrics WHERE project_id = %s", (PROJECT_ID,))
        conn.execute("DELETE FROM metric_definitions WHERE project_id = %s", (PROJECT_ID,))
        conn.execute("DELETE FROM metric_results WHERE project_id = %s", (PROJECT_ID,))
        conn.execute("DELETE FROM projects WHERE id = %s", (PROJECT_ID,))


def create_project(pool_manager: ConnectionPoolManager) -> None:
    pool = pool_manager.pool()
    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO projects (id, name, description) VALUES (%s, %s, %s)",
            (PROJECT_ID, PROJECT_NAME, PROJECT_DESC),
        )


def add_user(pool_manager: ConnectionPoolManager) -> None:
    user_store = PostgresUserStore(pool_manager)
    user = user_store.get_by_email(USER_EMAIL)
    if user is None:
        print(f"  User {USER_EMAIL} not found in database, skipping member assignment")
        return

    project_store = PostgresProjectStore(pool_manager)
    from kpidebug.management.types import Role
    project_store.add_member(PROJECT_ID, user.id, Role.ADMIN, user.name, user.email)
    print(f"  Added user {user.name} ({user.email}) as ADMIN")


def create_data_sources(pool_manager: ConnectionPoolManager) -> None:
    import json
    pool = pool_manager.pool()
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    with pool.connection() as conn:
        conn.execute(
            "INSERT INTO data_sources (id, project_id, name, type, credentials) VALUES (%s, %s, %s, %s, %s)",
            (STRIPE_SOURCE_ID, PROJECT_ID, "Stripe (Demo)", DataSourceType.STRIPE.value, json.dumps({"api_key": "sk_test_demo"})),
        )
        conn.execute(
            "INSERT INTO data_sources (id, project_id, name, type, credentials) VALUES (%s, %s, %s, %s, %s)",
            (GA_SOURCE_ID, PROJECT_ID, "Google Analytics (Demo)", DataSourceType.GOOGLE_ANALYTICS.value,
             json.dumps({"service_account_json": "{}", "property_id": "000000000"})),
        )

    print(f"  Created Stripe data source: {STRIPE_SOURCE_ID}")
    print(f"  Created GA data source: {GA_SOURCE_ID}")


def load_csv_data(pool_manager: ConnectionPoolManager) -> None:
    data_source_store = PostgresDataSourceStore(pool_manager)
    total_rows = 0

    for csv_file, (table_key, source_id) in CSV_TO_TABLE.items():
        filepath = DATA_DIR / csv_file
        if not filepath.exists():
            print(f"  WARNING: {csv_file} not found, skipping")
            continue

        rows = load_csv(filepath)
        data_source_store.set_cached_rows(source_id, table_key, rows)
        total_rows += len(rows)
        print(f"  {table_key}: {len(rows)} rows")

    print(f"  Total: {total_rows} rows loaded")


def pin_dashboard_metrics(pool_manager: ConnectionPoolManager) -> None:
    dashboard_store = PostgresDashboardStore(pool_manager)
    for metric_id, aggregation in DASHBOARD_METRICS:
        dashboard_store.add_metric(PROJECT_ID, metric_id, aggregation)
        print(f"  Pinned {metric_id} ({aggregation.value})")

    print(f"  {len(DASHBOARD_METRICS)} metrics pinned to dashboard")


def compute_snapshots(pool_manager: ConnectionPoolManager) -> None:
    from kpidebug.processor import process_all, ProcessMode

    data_source_store = PostgresDataSourceStore(pool_manager)
    dashboard_store = PostgresDashboardStore(pool_manager)
    metric_store = PostgresMetricStore(pool_manager)

    process_all(
        project_id=PROJECT_ID,
        data_source_store=data_source_store,
        dashboard_store=dashboard_store,
        metric_store=metric_store,
        mode=ProcessMode.METRICS,
    )


def main() -> None:
    if not DATA_DIR.exists():
        print(f"ERROR: Test data directory not found: {DATA_DIR}")
        print("Run 'python scripts/startup_demo/generate_testdata.py' first.")
        sys.exit(1)

    pool_manager = ConnectionPoolManager()

    try:
        print("Step 1: Cleaning up existing project...")
        cleanup_project(pool_manager)
        print("  Done")

        print(f"\nStep 2: Creating project '{PROJECT_NAME}' (id={PROJECT_ID})...")
        create_project(pool_manager)
        print("  Done")

        print("\nStep 3: Adding user...")
        add_user(pool_manager)

        print("\nStep 4: Creating data sources...")
        create_data_sources(pool_manager)

        print("\nStep 5: Loading CSV data...")
        load_csv_data(pool_manager)

        print("\nStep 6: Pinning dashboard metrics...")
        pin_dashboard_metrics(pool_manager)

        print("\nStep 7: Computing metric snapshots...")
        compute_snapshots(pool_manager)

        print(f"\nSetup complete! Project ID: {PROJECT_ID}")
        print("\nTo run analysis:")
        print(f"  python scripts/simulate.py {PROJECT_ID}")
        print(f"  python scripts/simulate.py {PROJECT_ID} --date 2026-02-28")

    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
