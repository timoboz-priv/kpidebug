import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from kpidebug.common.logging import init_logging
init_logging()

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.user_store_postgres import PostgresUserStore
from kpidebug.management.project_store_postgres import PostgresProjectStore
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.management.artifact_store_postgres import PostgresArtifactStore
from kpidebug.metrics.dashboard_store_postgres import PostgresDashboardStore
from kpidebug.analysis.insight_store_postgres import PostgresInsightStore


def get_stores(pool_manager: ConnectionPoolManager) -> list:
    return [
        PostgresUserStore(pool_manager),
        PostgresProjectStore(pool_manager),
        PostgresMetricStore(pool_manager),
        PostgresDataSourceStore(pool_manager),
        PostgresDashboardStore(pool_manager),
        PostgresArtifactStore(pool_manager),
        PostgresInsightStore(pool_manager),
    ]


def ensure_tables(pool_manager: ConnectionPoolManager) -> None:
    for store in get_stores(pool_manager):
        store.ensure_tables()


def drop_tables(pool_manager: ConnectionPoolManager) -> None:
    for store in reversed(get_stores(pool_manager)):
        store.drop_tables()


def clean(pool_manager: ConnectionPoolManager) -> None:
    for store in reversed(get_stores(pool_manager)):
        store.clean()


def seed_demo(pool_manager: ConnectionPoolManager) -> None:
    from kpidebug.management.types import User
    from kpidebug.data.types import DataSourceType

    user_store = PostgresUserStore(pool_manager)
    project_store = PostgresProjectStore(pool_manager)
    data_source_store = PostgresDataSourceStore(pool_manager)

    user = User(
        id="a9C5hdttpbZu4GmO1VAP7Ce4VS42",
        name="Timo Bozsolik-Torres",
        email="timo.boz@gmail.com",
        avatar_url="https://media.licdn.com/dms/image/v2/C5103AQF27pR03GqZyA/profile-displayphoto-shrink_400_400/profile-displayphoto-shrink_400_400/0/1516523638408?e=1778112000&v=beta&t=hUJaXGcF3JcvT3L2iyO0fEYjCft5ZMOhLGVCCWlPSTE",
    )
    user_store.create(user)
    print(f"Created user: {user.name} ({user.email})")

    project = project_store.create(
        name="QF Test",
        description="Quantifiction test using Stripe, analytics, ...",
        creator_id=user.id,
        creator_name=user.name,
        creator_email=user.email,
    )
    print(f"Created project: {project.name} (id={project.id})")

    source = data_source_store.create_source(
        project_id=project.id,
        name="Stripe",
        source_type=DataSourceType.STRIPE,
        credentials={"api_key": os.getenv("STRIPE_API_KEY", "")},
    )
    print(f"Created Stripe data source: {source.name} (id={source.id})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Set up the KPI Debugger database")
    parser.add_argument(
        "action",
        choices=["create", "drop", "reset", "clean"],
        help="create: create tables, drop: drop tables, reset: drop + create, clean: delete all data",
    )
    parser.add_argument(
        "--mode",
        choices=["base", "demo"],
        default="base",
        help="base: empty tables only, demo: seed with demo user, project, and Stripe source",
    )
    args = parser.parse_args()

    pool_manager = ConnectionPoolManager()

    try:
        if args.action == "create":
            ensure_tables(pool_manager)
            print("Tables created.")
        elif args.action == "drop":
            drop_tables(pool_manager)
            print("Tables dropped.")
        elif args.action == "reset":
            drop_tables(pool_manager)
            ensure_tables(pool_manager)
            print("Tables reset.")
        elif args.action == "clean":
            clean(pool_manager)
            print("All data deleted.")

        if args.mode == "demo":
            seed_demo(pool_manager)
    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
