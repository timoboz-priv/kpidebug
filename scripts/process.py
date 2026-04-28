import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from kpidebug.common.logging import init_logging
init_logging()

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.metrics.dashboard_store_postgres import PostgresDashboardStore
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore
from kpidebug.processor import ProcessMode, process_all


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the processing pipeline for a project",
    )
    parser.add_argument(
        "project_id",
        help="Project ID to process",
    )
    parser.add_argument(
        "--mode",
        choices=[m.value for m in ProcessMode],
        default=ProcessMode.FULL.value,
        help="Processing mode: full (sync+metrics+analysis), metrics (skip sync), analysis (skip sync+metrics). Default: full.",
    )
    args = parser.parse_args()

    pool_manager = ConnectionPoolManager()

    try:
        process_all(
            project_id=args.project_id,
            data_source_store=PostgresDataSourceStore(pool_manager),
            dashboard_store=PostgresDashboardStore(pool_manager),
            metric_store=PostgresMetricStore(pool_manager),
            mode=ProcessMode(args.mode),
        )
    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
