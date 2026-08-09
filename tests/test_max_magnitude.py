from __future__ import annotations

from jp_seismic.extract import max_magnitude


def _feature(mag: object) -> dict:
    return {"id": "us1", "properties": {"mag": mag}}


def test_max_magnitude_returns_the_largest_value():
    assert max_magnitude([_feature(3.1), _feature(6.4), _feature(2.0)]) == 6.4


def test_max_magnitude_on_empty_input_is_none():
    assert max_magnitude([]) is None


def test_max_magnitude_ignores_features_without_a_magnitude():
    assert max_magnitude([_feature(None), {"properties": {}}, _feature(4.2)]) == 4.2


def test_max_magnitude_when_no_feature_carries_one_is_none():
    assert max_magnitude([_feature(None), {"id": "us2"}]) is None


def test_max_magnitude_keeps_magnitudes_at_or_below_zero():
    # USGS publishes negative magnitudes for the smallest events; discarding
    # them would silently misreport a quiet window as having no data.
    assert max_magnitude([_feature(-0.4), _feature(0.0)]) == 0.0


def test_max_magnitude_ignores_non_numeric_values():
    assert max_magnitude([_feature("6.0"), _feature(True), _feature(3.3)]) == 3.3
