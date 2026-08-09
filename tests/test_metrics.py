from __future__ import annotations

import pytest

from jp_seismic.config import MonitoringConfig
from jp_seismic.metrics import IngestRun, publish_ingest_run


@pytest.fixture
def run() -> IngestRun:
    return IngestRun(
        rows_read=12,
        rows_written=10,
        rows_rejected=2,
        duration_seconds=4.5,
        succeeded=True,
    )


def test_publish_ingest_run_without_a_gateway_pushes_nothing(monkeypatch, run):
    def fail(*_args, **_kwargs):
        raise AssertionError("push_to_gateway must not be called")

    monkeypatch.setattr("jp_seismic.metrics.push_to_gateway", fail)

    publish_ingest_run(run, MonitoringConfig(pushgateway_url=None, job_name="ingest"))


def test_publish_ingest_run_pushes_the_four_run_numbers(monkeypatch, run):
    pushed = {}

    def capture(url, *, job, registry, grouping_key):
        pushed["url"] = url
        pushed["job"] = job
        pushed["grouping_key"] = grouping_key
        pushed["registry"] = registry

    monkeypatch.setattr("jp_seismic.metrics.push_to_gateway", capture)

    publish_ingest_run(
        run,
        MonitoringConfig(pushgateway_url="http://gw:9091", job_name="ingest"),
        source="usgs",
    )

    registry = pushed["registry"]
    assert pushed["url"] == "http://gw:9091"
    assert pushed["job"] == "ingest"
    assert pushed["grouping_key"] == {"source": "usgs"}
    assert registry.get_sample_value("jp_seismic_ingest_rows_read") == 12
    assert registry.get_sample_value("jp_seismic_ingest_rows_written") == 10
    assert registry.get_sample_value("jp_seismic_ingest_rows_rejected") == 2
    assert registry.get_sample_value("jp_seismic_ingest_duration_seconds") == 4.5
    assert registry.get_sample_value("jp_seismic_ingest_last_run_success") == 1


def test_publish_ingest_run_omits_success_timestamp_when_the_run_failed(monkeypatch):
    pushed = {}
    monkeypatch.setattr(
        "jp_seismic.metrics.push_to_gateway",
        lambda url, *, job, registry, grouping_key: pushed.update(registry=registry),
    )

    publish_ingest_run(
        IngestRun(
            rows_read=3,
            rows_written=0,
            rows_rejected=0,
            duration_seconds=1.0,
            succeeded=False,
        ),
        MonitoringConfig(pushgateway_url="http://gw:9091", job_name="ingest"),
    )

    registry = pushed["registry"]
    assert registry.get_sample_value("jp_seismic_ingest_last_run_success") == 0
    # A failed run must not advance the freshness clock, or the staleness
    # alert would be reset by the very failures it exists to catch.
    assert (
        registry.get_sample_value("jp_seismic_ingest_last_success_timestamp_seconds")
        is None
    )


def test_publish_ingest_run_survives_an_unreachable_gateway(monkeypatch, caplog, run):
    def unreachable(*_args, **_kwargs):
        raise OSError("connection refused")

    monkeypatch.setattr("jp_seismic.metrics.push_to_gateway", unreachable)

    publish_ingest_run(
        run, MonitoringConfig(pushgateway_url="http://gw:9091", job_name="ingest")
    )

    assert "could not push metrics" in caplog.text


def test_monitoring_config_treats_an_empty_url_as_unconfigured(monkeypatch):
    monkeypatch.setenv("PUSHGATEWAY_URL", "   ")

    assert MonitoringConfig.from_env().pushgateway_url is None
