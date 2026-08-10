from __future__ import annotations

from typing import cast

import psycopg

from jp_seismic.load import BATCH_SIZE, LoadResult, load_events


class FakeCursor:
    """Records executemany batches instead of talking to Postgres."""

    def __init__(self) -> None:
        self.batches: list[list[tuple]] = []
        self.rowcount = 0

    def executemany(self, _sql: str, rows: list[tuple]) -> None:
        self.batches.append(list(rows))
        self.rowcount = len(rows)

    def __enter__(self) -> FakeCursor:
        return self

    def __exit__(self, *_exc: object) -> None:
        return None


class FakeConnection:
    def __init__(self) -> None:
        self.cur = FakeCursor()
        self.commits = 0

    def cursor(self) -> FakeCursor:
        return self.cur

    def commit(self) -> None:
        self.commits += 1


def _feature(event_id: str) -> dict:
    return {"id": event_id, "properties": {"mag": 3.1}, "geometry": {}}


def _load(
    conn: FakeConnection,
    features: list[dict],
    source: str = "usgs",
) -> LoadResult:
    # The fake stands in for a psycopg connection at the system boundary. The
    # cast records that deliberately, instead of loosening load_events' own
    # signature to accommodate a test double.
    return load_events(cast("psycopg.Connection", conn), features, source=source)


def test_load_events_writes_all_features_and_commits() -> None:
    conn = FakeConnection()

    result = _load(conn, [_feature("us1"), _feature("us2")])

    assert result.written == 2
    assert conn.commits == 1
    assert [row[0] for row in conn.cur.batches[0]] == ["us1", "us2"]


def test_load_events_skips_features_without_an_id() -> None:
    conn = FakeConnection()

    result = _load(conn, [_feature("us1"), {"properties": {}}, _feature("us2")])

    assert result.written == 2
    assert [row[0] for row in conn.cur.batches[0]] == ["us1", "us2"]


def test_load_events_counts_features_without_an_id_as_rejected() -> None:
    conn = FakeConnection()

    result = _load(conn, [_feature("us1"), {"properties": {}}, {"id": ""}])

    assert result.rejected == 2


def test_load_events_splits_large_inputs_into_batches() -> None:
    conn = FakeConnection()
    features = [_feature(f"us{i}") for i in range(BATCH_SIZE + 25)]

    _load(conn, features)

    assert [len(b) for b in conn.cur.batches] == [BATCH_SIZE, 25]


def test_load_events_tags_rows_with_source() -> None:
    conn = FakeConnection()

    _load(conn, [_feature("us1")], source="jma")

    assert conn.cur.batches[0][0][2] == "jma"


def test_load_events_on_empty_input_writes_nothing() -> None:
    conn = FakeConnection()

    assert _load(conn, []).written == 0
    assert conn.cur.batches == []
