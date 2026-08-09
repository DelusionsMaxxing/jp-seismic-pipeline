"""Command-line entrypoint for ingesting a date range.

Used both by the Airflow DAG and for manual backfills:

    python -m jp_seismic.cli --start 2024-01-01 --end 2024-02-01
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta

import psycopg
import requests

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


@dataclass(frozen=True, slots=True)
class _WindowOutcome:
    rows_read: int
    rows_written: int
    rows_rejected: int
    max_magnitude: float | None


def _parse_date(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"expected YYYY-MM-DD, got {value!r}") from exc


def _load_window(
    conn: psycopg.Connection,
    session: requests.Session,
    start: date,
    end: date,
    source: str,
) -> _WindowOutcome:
    features = fetch_events(start, end, session=session)
    loaded = load_events(conn, features, source=source)

    return _WindowOutcome(
        rows_read=len(features),
        rows_written=loaded.written,
        rows_rejected=loaded.rejected,
        max_magnitude=max_magnitude(features),
    )


def _summarise(
    outcomes: Sequence[_WindowOutcome],
    duration_seconds: float,
    succeeded: bool,
) -> IngestRun:
    # Filtered on None rather than truthiness: USGS reports magnitudes of zero
    # and below for the smallest events, and those are real values.
    magnitudes = [
        outcome.max_magnitude
        for outcome in outcomes
        if outcome.max_magnitude is not None
    ]

    return IngestRun(
        rows_read=sum(outcome.rows_read for outcome in outcomes),
        rows_written=sum(outcome.rows_written for outcome in outcomes),
        rows_rejected=sum(outcome.rows_rejected for outcome in outcomes),
        duration_seconds=duration_seconds,
        succeeded=succeeded,
        max_magnitude=max(magnitudes) if magnitudes else None,
    )


def ingest(
    start: date,
    end: date,
    window_days: int = 30,
    source: str = "usgs",
) -> int:
    """Return rows written."""
    database = DatabaseConfig.from_env()
    monitoring = MonitoringConfig.from_env()
    session = build_session()

    started = time.monotonic()
    outcomes: list[_WindowOutcome] = []
    succeeded = False

    try:
        with psycopg.connect(database.dsn) as conn:
            for window_start, window_end in iter_backfill_windows(
                start, end, window_days=window_days
            ):
                outcomes.append(
                    _load_window(conn, session, window_start, window_end, source)
                )
        succeeded = True
    finally:
        # Published from finally so a run that raised still reports what it got
        # through: a run that dies silently is indistinguishable from one that
        # never started.
        run = _summarise(outcomes, time.monotonic() - started, succeeded)
        publish_ingest_run(run, monitoring, source=source)

    logger.info(
        "ingest complete: %d read, %d written, %d rejected for %s..%s",
        run.rows_read,
        run.rows_written,
        run.rows_rejected,
        start,
        end,
    )
    return run.rows_written


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

    # Default to yesterday: USGS keeps revising the current day's solutions, so
    # a completed UTC day is the earliest point worth treating as stable.
    today = datetime.now(UTC).date()
    end = args.end or today
    start = args.start or end - timedelta(days=1)

    ingest(start, end, window_days=args.window_days)
    return 0


if __name__ == "__main__":
    sys.exit(main())
