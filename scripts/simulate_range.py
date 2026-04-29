"""Simulate analysis across a date range and collect all insights.

Runs the analysis engine for each day in the range, deduplicates
insights by headline, and prints a timeline of issues found.

Usage:
    python scripts/simulate_range.py test-startup-demo \\
        --start 2025-05-01 --end 2026-04-28
    python scripts/simulate_range.py test-startup-demo \\
        --start 2025-06-01 --end 2025-06-30
"""

import argparse
import sys
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

from kpidebug.common.logging import init_logging
init_logging()

import logging
logging.getLogger("kpidebug").setLevel(logging.WARNING)

from kpidebug.common.db import ConnectionPoolManager
from kpidebug.data.data_source_store_postgres import PostgresDataSourceStore
from kpidebug.metrics.dashboard_store_postgres import PostgresDashboardStore
from kpidebug.metrics.metric_store_postgres import PostgresMetricStore
from kpidebug.processor import process_simulate


@dataclass
class InsightOccurrence:
    headline: str = ""
    first_seen: date = field(default_factory=date.today)
    last_seen: date = field(default_factory=date.today)
    days_active: int = 0
    peak_confidence: float = 0.0
    peak_date: date = field(default_factory=date.today)
    signals_sample: list[str] = field(default_factory=list)
    actions_sample: list[str] = field(default_factory=list)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Simulate analysis across a date range",
    )
    parser.add_argument("project_id", help="Project ID")
    parser.add_argument("--start", type=date.fromisoformat, required=True)
    parser.add_argument("--end", type=date.fromisoformat, required=True)
    args = parser.parse_args()

    pool_manager = ConnectionPoolManager()

    try:
        data_source_store = PostgresDataSourceStore(pool_manager)
        dashboard_store = PostgresDashboardStore(pool_manager)
        metric_store = PostgresMetricStore(pool_manager)

        occurrences: dict[str, InsightOccurrence] = {}
        timeline: list[tuple[date, list[str]]] = []
        total_days = (args.end - args.start).days + 1

        print(f"Simulating {total_days} days: {args.start} to {args.end}")
        print()

        d = args.start
        while d <= args.end:
            result = process_simulate(
                project_id=args.project_id,
                data_source_store=data_source_store,
                dashboard_store=dashboard_store,
                metric_store=metric_store,
                as_of_date=d,
            )

            day_headlines: list[str] = []
            for insight in result.insights:
                day_headlines.append(insight.headline)
                occ = occurrences.get(insight.headline)
                if occ is None:
                    occ = InsightOccurrence(
                        headline=insight.headline,
                        first_seen=d,
                        last_seen=d,
                        days_active=1,
                        peak_confidence=insight.confidence.score,
                        peak_date=d,
                        signals_sample=[s.description for s in insight.signals],
                        actions_sample=[a.description for a in insight.actions],
                    )
                    occurrences[insight.headline] = occ
                else:
                    occ.last_seen = d
                    occ.days_active += 1
                    if insight.confidence.score > occ.peak_confidence:
                        occ.peak_confidence = insight.confidence.score
                        occ.peak_date = d
                        occ.signals_sample = [s.description for s in insight.signals]

            if day_headlines:
                timeline.append((d, day_headlines))

            progress = (d - args.start).days + 1
            bar_len = 40
            filled = int(bar_len * progress / total_days)
            bar = "█" * filled + "░" * (bar_len - filled)
            count = len(day_headlines)
            marker = f" → {count} insight(s)" if count else ""
            print(f"\r  {bar} {progress}/{total_days} [{d}]{marker}", end="", flush=True)

            d += timedelta(days=1)

        print("\n")

        if not occurrences:
            print("No insights found in the entire range.")
            return

        sorted_occ = sorted(occurrences.values(), key=lambda o: o.first_seen)

        print(f"{'=' * 72}")
        print(f"  {len(sorted_occ)} unique insight(s) found across {len(timeline)} day(s)")
        print(f"{'=' * 72}")

        for i, occ in enumerate(sorted_occ, 1):
            duration = (occ.last_seen - occ.first_seen).days + 1
            print(f"\n  {i}. {occ.headline}")
            print(f"     Active: {occ.first_seen} to {occ.last_seen} ({occ.days_active} of {duration} days)")
            print(f"     Peak confidence: {occ.peak_confidence * 100:.0f}% on {occ.peak_date}")
            print(f"     Signals:")
            for sig in occ.signals_sample:
                print(f"       - {sig}")
            print(f"     Actions:")
            for act in occ.actions_sample:
                print(f"       - {act}")

        print(f"\n{'=' * 72}")
        print(f"  Timeline")
        print(f"{'=' * 72}\n")

        prev_date = None
        for d, headlines in timeline:
            if prev_date and (d - prev_date).days > 1:
                gap = (d - prev_date).days - 1
                print(f"  ... {gap} day(s) quiet ...")
            for h in headlines:
                print(f"  {d}  {h}")
            prev_date = d

        print()

    finally:
        pool_manager.close()


if __name__ == "__main__":
    main()
