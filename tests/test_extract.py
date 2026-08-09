from __future__ import annotations

from datetime import date
from itertools import pairwise

import pytest
import responses

from jp_seismic.config import USGS_ENDPOINT
from jp_seismic.extract import PAGE_LIMIT, fetch_events, iter_backfill_windows


def _feature(event_id: str) -> dict:
    return {
        "type": "Feature",
        "id": event_id,
        "properties": {
            "mag": 4.2,
            "time": 1786128091903,
            "place": "1 km N of X, Japan",
        },
        "geometry": {"type": "Point", "coordinates": [140.68, 36.72, 10]},
    }


@responses.activate
def test_fetch_events_returns_features_from_single_page():
    responses.get(
        USGS_ENDPOINT,
        json={"features": [_feature("us1"), _feature("us2")]},
        status=200,
    )

    events = fetch_events(date(2024, 1, 1), date(2024, 1, 2))

    assert [e["id"] for e in events] == ["us1", "us2"]
    assert len(responses.calls) == 1


@responses.activate
def test_fetch_events_pages_until_short_page():
    full_page = [_feature(f"us{i}") for i in range(PAGE_LIMIT)]
    responses.get(USGS_ENDPOINT, json={"features": full_page}, status=200)
    responses.get(USGS_ENDPOINT, json={"features": [_feature("last")]}, status=200)

    events = fetch_events(date(2024, 1, 1), date(2024, 2, 1))

    assert len(events) == PAGE_LIMIT + 1
    assert len(responses.calls) == 2
    # FDSN offsets are 1-based; the second page must start after the first.
    assert "offset=1&" in responses.calls[0].request.url.replace("?", "&")
    assert f"offset={PAGE_LIMIT + 1}" in responses.calls[1].request.url


@responses.activate
def test_fetch_events_treats_204_as_empty_window():
    responses.get(USGS_ENDPOINT, status=204)

    assert fetch_events(date(2024, 1, 1), date(2024, 1, 2)) == []


@responses.activate
def test_fetch_events_constrains_query_to_japan_bbox():
    responses.get(USGS_ENDPOINT, json={"features": []}, status=200)

    fetch_events(date(2024, 1, 1), date(2024, 1, 2))

    url = responses.calls[0].request.url
    assert "minlatitude=24.0" in url
    assert "maxlatitude=46.5" in url
    assert "minlongitude=122.0" in url
    assert "maxlongitude=154.0" in url


def test_fetch_events_rejects_inverted_range():
    with pytest.raises(ValueError, match="must be after"):
        fetch_events(date(2024, 2, 1), date(2024, 1, 1))


def test_iter_backfill_windows_covers_range_without_gaps_or_overlap():
    windows = list(
        iter_backfill_windows(date(2024, 1, 1), date(2024, 3, 1), window_days=30)
    )

    assert windows[0][0] == date(2024, 1, 1)
    assert windows[-1][1] == date(2024, 3, 1)
    for (_, prev_end), (next_start, _) in pairwise(windows):
        assert prev_end == next_start


def test_iter_backfill_windows_clamps_final_window_to_end():
    windows = list(
        iter_backfill_windows(date(2024, 1, 1), date(2024, 1, 10), window_days=30)
    )

    assert windows == [(date(2024, 1, 1), date(2024, 1, 10))]


def test_iter_backfill_windows_rejects_non_positive_window():
    with pytest.raises(ValueError, match="at least 1"):
        list(iter_backfill_windows(date(2024, 1, 1), date(2024, 2, 1), window_days=0))
