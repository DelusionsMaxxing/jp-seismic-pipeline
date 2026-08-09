"""Command-line entrypoint for ingesting a date range.

Used both by the Airflow DAG and for manual backfills:

    python -m jp_seismic.cli --start 2024-01-01 --end 2024-02-01
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime, timedelta

import psycopg

from .config import DatabaseConfig
from .extract import build_session, fetch_events, iter_backfill_windows
from .load import load_events

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(
            f"expected YYYY-MM-DD, got {value!r}"
        ) from exc


def ingest(start: date, end: date, window_days: int = 30) -> int:
    """Fetch and load every event in ``[start, end)``; return rows written."""
    db = DatabaseConfig.from_env()
    session = build_session()
    total = 0

    with psycopg.connect(db.dsn) as conn:
        for window_start, window_end in iter_backfill_windows(
            start, end, window_days=window_days
        ):
            features = fetch_events(window_start, window_end, session=session)
            total += load_events(conn, features)

    logger.info("ingest complete: %d rows written for %s..%s", total, start, end)
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start", type=_parse_date, help="inclusive, YYYY-MM-DD")
    parser.add_argument("--end", type=_parse_date, help="exclusive, YYYY-MM-DD")
    parser.add_argument(
        "--window-days",
        type=int,
        default=30,
        help="chunk size for long backfills (default: 30)",
    )
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)-8s %(name)s | %(message)s",
    )

    # Default to yesterday: USGS keeps revising the current day's solutions,
    # so a completed UTC day is the earliest point worth treating as stable.
    today = datetime.now(UTC).date()
    end = args.end or today
    start = args.start or end - timedelta(days=1)

    ingest(start, end, window_days=args.window_days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
