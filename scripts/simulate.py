import argparse
import sys
from datetime import date
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
from kpidebug.processor import process_simulate


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate metric computation and analysis for a project at a given date",
    )
    parser.add_argument(
        "project_id",
        help="Project ID to simulate",
    )
    parser.add_argument(
        "--date",
        type=date.fromisoformat,
        default=None,
        help="As-of date in ISO format (YYYY-MM-DD). Defaults to today.",
    )
    args = parser.parse_args()

    pool_manager = ConnectionPoolManager()

    try:
        data_source_store = PostgresDataSourceStore(pool_manager)
        dashboard_store = PostgresDashboardStore(pool_manager)
        metric_store = PostgresMetricStore(pool_manager)

        result = process_simulate(
            project_id=args.project_id,
            data_source_store=data_source_store,
            dashboard_store=dashboard_store,
            metric_store=metric_store,
            as_of_date=args.date,
        )

        if not result.insights:
            print("No insights found.")
        else:
            for insight in result.insights:
                print(f"\n{'=' * 60}")
                print(f"  {insight.headline}")
                print(f"  {insight.description}")
                if insight.confidence.score > 0:
                    pct = insight.confidence.score * 100
                    print(f"  Confidence: {pct:.0f}% — {insight.confidence.description}")
                print(f"  Signals:")
                for signal in insight.signals:
                    print(f"    - {signal.description}")
                print(f"  Actions:")
                for action in insight.actions:
                    print(f"    [{action.priority.value}] {action.description}")
                if insight.revenue_impact.value > 0:
                    print(f"  {insight.revenue_impact.description}")
                if insight.counterfactual.value > 0:
                    desc = insight.counterfactual.description
                    if insight.counterfactual.revenue_impact.value > 0:
                        desc += f" | {insight.counterfactual.revenue_impact.description}"
                    print(f"  {desc}")
    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
