"""Idempotent loading of raw GeoJSON features into the landing table."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from typing import Any

import psycopg
from psycopg.types.json import Jsonb

logger = logging.getLogger(__name__)

Feature = dict[str, Any]

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


def _rows(features: Iterable[Feature], source: str) -> Iterable[Sequence[Any]]:
    for feature in features:
        event_id = feature.get("id")
        if not event_id:
            # Without a stable id there is nothing to deduplicate on, so the
            # event would multiply on every re-run.
            logger.warning("skipping feature with no id: %s", json.dumps(feature)[:200])
            continue
        yield (event_id, Jsonb(feature), source)


def load_events(
    conn: psycopg.Connection,
    features: Iterable[Feature],
    source: str = "usgs",
) -> int:
    """Upsert ``features`` into ``raw.earthquake_events``; return rows written."""
    batch: list[Sequence[Any]] = []
    written = 0

    with conn.cursor() as cur:
        for row in _rows(features, source):
            batch.append(row)
            if len(batch) >= BATCH_SIZE:
                cur.executemany(UPSERT_SQL, batch)
                written += cur.rowcount if cur.rowcount > 0 else 0
                batch.clear()

        if batch:
            cur.executemany(UPSERT_SQL, batch)
            written += cur.rowcount if cur.rowcount > 0 else 0

    conn.commit()
    logger.info("upserted %d rows into raw.earthquake_events", written)
    return written
