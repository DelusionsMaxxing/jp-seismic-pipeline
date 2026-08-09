"""Extraction of raw seismic events from the USGS FDSN event API."""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import date, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import JAPAN_BBOX, PAGE_LIMIT, REQUEST_TIMEOUT_SECONDS, USGS_ENDPOINT

logger = logging.getLogger(__name__)

Feature = dict[str, Any]


def build_session() -> requests.Session:
    """A session that retries idempotent GETs with exponential backoff.

    USGS rate-limits aggressively during swarm events, which is exactly when
    a backfill is most likely to be running, so 429 is treated as retryable.
    """
    session = requests.Session()
    retry = Retry(
        total=5,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset(["GET"]),
        raise_on_status=False,
    )
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def fetch_events(
    start: date,
    end: date,
    session: requests.Session | None = None,
) -> list[Feature]:
    """Return every event in the Japan bounding box for ``[start, end)``.

    The API's ``endtime`` is inclusive of the instant, not the day, so the
    caller's half-open interval is preserved by requesting up to midnight of
    ``end``. Results are paged with ``offset`` because a single response is
    capped at :data:`PAGE_LIMIT` features.
    """
    if end <= start:
        raise ValueError(f"end ({end}) must be after start ({start})")

    session = session or build_session()
    features: list[Feature] = []
    # FDSN offsets are 1-based, unlike almost every other paging API.
    offset = 1

    while True:
        params = {
            "format": "geojson",
            "starttime": start.isoformat(),
            "endtime": end.isoformat(),
            "limit": PAGE_LIMIT,
            "offset": offset,
            "orderby": "time-asc",
            **JAPAN_BBOX,
        }
        response = session.get(
            USGS_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT_SECONDS
        )
        # A window with no events answers 204 with an empty body, which is a
        # success case rather than something to raise on.
        if response.status_code == 204:
            break
        response.raise_for_status()

        page = response.json().get("features", [])
        features.extend(page)
        logger.info(
            "fetched %d events (offset=%d) for %s..%s", len(page), offset, start, end
        )

        if len(page) < PAGE_LIMIT:
            break
        offset += PAGE_LIMIT

    return features


def iter_backfill_windows(
    start: date, end: date, window_days: int = 30
) -> Iterator[tuple[date, date]]:
    """Split a long backfill into windows small enough to stay under the cap."""
    if window_days < 1:
        raise ValueError("window_days must be at least 1")

    cursor = start
    while cursor < end:
        window_end = min(cursor + timedelta(days=window_days), end)
        yield cursor, window_end
        cursor = window_end
