"""Extraction of raw seismic events from the USGS FDSN event API."""

from __future__ import annotations

import logging
from collections.abc import Iterable, Iterator
from datetime import date, timedelta
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .config import JAPAN_BBOX, PAGE_LIMIT, REQUEST_TIMEOUT_SECONDS, USGS_ENDPOINT

logger = logging.getLogger(__name__)

Feature = dict[str, Any]


def build_session() -> requests.Session:
    session = requests.Session()
    # USGS rate-limits aggressively during swarm events, which is exactly when
    # a backfill is most likely to be running, so 429 is retryable here.
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
    if end <= start:
        raise ValueError(f"end ({end}) must be after start ({start})")

    session = session or build_session()
    features: list[Feature] = []
    # FDSN offsets are 1-based, unlike almost every other paging API.
    offset = 1

    while True:
        # endtime is inclusive of the instant rather than of the day, so
        # requesting midnight of `end` preserves the caller's half-open range.
        params: dict[str, str | int | float] = {
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


def max_magnitude(features: Iterable[Feature]) -> float | None:
    # Magnitudes arrive from an external API, so anything non-numeric is
    # ignored rather than allowed to fail an otherwise healthy run.
    magnitudes = [
        value
        for feature in features
        if isinstance(value := feature.get("properties", {}).get("mag"), int | float)
        and not isinstance(value, bool)
    ]
    return max(magnitudes) if magnitudes else None


def iter_backfill_windows(
    start: date, end: date, window_days: int = 30
) -> Iterator[tuple[date, date]]:
    if window_days < 1:
        raise ValueError("window_days must be at least 1")

    cursor = start
    while cursor < end:
        window_end = min(cursor + timedelta(days=window_days), end)
        yield cursor, window_end
        cursor = window_end
