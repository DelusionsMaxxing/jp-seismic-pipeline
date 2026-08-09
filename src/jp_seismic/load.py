"""Idempotent loading of raw GeoJSON features into the landing table."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

Feature = dict[str, Any]
Row = tuple[str, Jsonb, str]

# The landing table keeps the payload whole: parsing belongs in dbt, where it
# is versioned and testable. Re-running a window updates in place, so the DAG
# stays safe to retry and USGS's later magnitude revisions are picked up.
UPSERT_SQL = """
INSERT INTO raw.earthquake_events (event_id, payload, source, ingested_at, updated_at)
VALUES (%s, %s, %s, now(), now())
ON CONFLICT (event_id) DO UPDATE
SET payload    = EXCLUDED.payload,
    updated_at = now()
WHERE raw.earthquake_events.payload IS DISTINCT FROM EXCLUDED.payload
"""

BATCH_SIZE = 500


@dataclass(frozen=True, slots=True)
class LoadResult:
    written: int
    rejected: int


def _batched(rows: Sequence[Row], size: int) -> Iterator[Sequence[Row]]:
    for start in range(0, len(rows), size):
        yield rows[start : start + size]


def _rows_to_upsert(features: Iterable[Feature], source: str) -> tuple[list[Row], int]:
    rows: list[Row] = []
    rejected = 0

    for feature in features:
        event_id = feature.get("id")
        if not event_id:
            # Without a stable id there is nothing to deduplicate on, so the
            # event would multiply on every re-run. Counted rather than
            # silently dropped: a source that starts emitting unusable records
            # has to surface as a number, not as a quietly shrinking table.
            rejected += 1
            logger.warning("skipping feature with no id: %s", json.dumps(feature)[:200])
            continue

        rows.append((event_id, Jsonb(feature), source))

    return rows, rejected


def _upsert(conn: psycopg.Connection, rows: Sequence[Row]) -> int:
    written = 0

    with conn.cursor() as cur:
        for batch in _batched(rows, BATCH_SIZE):
            cur.executemany(UPSERT_SQL, batch)
            written += max(cur.rowcount, 0)

    conn.commit()
    return written


def load_events(
    conn: psycopg.Connection,
    features: Iterable[Feature],
    source: str = "usgs",
) -> LoadResult:
    rows, rejected = _rows_to_upsert(features, source)
    written = _upsert(conn, rows)

    logger.info(
        "upserted %d rows into raw.earthquake_events (%d rejected)", written, rejected
    )
    return LoadResult(written=written, rejected=rejected)
