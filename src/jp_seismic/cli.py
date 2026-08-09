"""Command-line entrypoint for ingesting a date range.

Used both by the Airflow DAG and for manual backfills:

    python -m jp_seismic.cli --start 2024-01-01 --end 2024-02-01
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from datetime import UTC, date, datetime, timedelta

import psycopg

from .config import DatabaseConfig, MonitoringConfig
from .extract import (
    build_session,
    fetch_events,
    iter_backfill_windows,
    max_magnitude,
)
from .load import load_events
from .metrics import IngestRun, publish_ingest_run

logger = logging.getLogger(__name__)


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def ingest(
    start: date,
    end: date,
    window_days: int = 30,
    source: str = "usgs",
) -> int:
    """Fetch and load every event in ``[start, end)``; return rows written."""
    db = DatabaseConfig.from_env()
    monitoring = MonitoringConfig.from_env()
    session = build_session()

    started = time.monotonic()
    rows_read = 0
    rows_written = 0
    rows_rejected = 0
    strongest: float | None = None
    succeeded = False

    try:
        with psycopg.connect(db.dsn) as conn:
            for window_start, window_end in iter_backfill_windows(
                start, end, window_days=window_days
            ):
                features = fetch_events(window_start, window_end, session=session)
                rows_read += len(features)

                window_max = max_magnitude(features)
                if window_max is not None:
                    # Compared against None explicitly: USGS reports magnitudes
                    # of zero and below for the smallest events, and a
                    # truthiness check would discard them.
                    strongest = (
                        window_max if strongest is None else max(strongest, window_max)
                    )

                result = load_events(conn, features, source=source)
                rows_written += result.written
                rows_rejected += result.rejected
        succeeded = True
    finally:
        # Published from `finally` so a failed run still records its numbers —
        # a run that dies silently is indistinguishable from one that never
        # started, which is the failure mode monitoring exists to remove.
        publish_ingest_run(
            IngestRun(
                rows_read=rows_read,
                rows_written=rows_written,
                rows_rejected=rows_rejected,
                duration_seconds=time.monotonic() - started,
                succeeded=succeeded,
                max_magnitude=strongest,
            ),
            monitoring,
            source=source,
        )

    logger.info(
        "ingest complete: %d read, %d written, %d rejected for %s..%s",
        rows_read,
        rows_written,
        rows_rejected,
        start,
        end,
    )
    return rows_written


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
