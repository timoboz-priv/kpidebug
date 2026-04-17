import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.management.user_store_postgres import PostgresUserStore
from kpidebug.management.project_store_postgres import PostgresProjectStore
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore
from kpidebug.data.data_store_postgres import PostgresDataStore
from kpidebug.data.cache.postgres import PostgresTableCache


def get_stores(pool_manager: ConnectionPoolManager) -> list:
    return [
        PostgresUserStore(pool_manager),
        PostgresProjectStore(pool_manager),
        PostgresMetricStore(pool_manager),
        PostgresDataStore(pool_manager),
        PostgresTableCache(pool_manager),
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


def main() -> None:
    parser = argparse.ArgumentParser(description="Set up the KPI Debugger database")
    parser.add_argument(
        "action",
        choices=["create", "drop", "reset", "clean"],
        help="create: create tables, drop: drop tables, reset: drop + create, clean: delete all data",
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
    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
