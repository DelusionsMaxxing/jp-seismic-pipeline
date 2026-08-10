from __future__ import annotations

from dataclasses import dataclass

import pytest
from prometheus_client import CollectorRegistry

from jp_seismic.config import MonitoringConfig
from jp_seismic.metrics import IngestRun, publish_ingest_run


@dataclass(frozen=True, slots=True)
class _Push:
    url: str
    job: str
    registry: CollectorRegistry
    grouping_key: dict[str, str]


@pytest.fixture
def run() -> IngestRun:
    return IngestRun(
        rows_read=12,
        rows_written=10,
        rows_rejected=2,
        duration_seconds=4.5,
        succeeded=True,
        max_magnitude=6.4,
    )


def test_publish_ingest_run_without_a_gateway_pushes_nothing(
    monkeypatch: pytest.MonkeyPatch, run: IngestRun
) -> None:
    def fail(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("push_to_gateway must not be called")

    monkeypatch.setattr("jp_seismic.metrics.push_to_gateway", fail)

    publish_ingest_run(run, MonitoringConfig(pushgateway_url=None, job_name="ingest"))


def test_publish_ingest_run_pushes_every_run_metric(
    monkeypatch: pytest.MonkeyPatch, run: IngestRun
) -> None:
    pushes: list[_Push] = []

    def capture(
        url: str,
        *,
        job: str,
        registry: CollectorRegistry,
        grouping_key: dict[str, str],
    ) -> None:
        pushes.append(
            _Push(url=url, job=job, registry=registry, grouping_key=grouping_key)
        )

    monkeypatch.setattr("jp_seismic.metrics.push_to_gateway", capture)

    publish_ingest_run(
        run,
        MonitoringConfig(pushgateway_url="http://gw:9091", job_name="ingest"),
        source="usgs",
    )

    assert len(pushes) == 1
    push = pushes[0]
    registry = push.registry
    assert push.url == "http://gw:9091"
    assert push.job == "ingest"
    assert push.grouping_key == {"source": "usgs"}
    assert registry.get_sample_value("jp_seismic_ingest_rows_read") == 12
    assert registry.get_sample_value("jp_seismic_ingest_rows_written") == 10
    assert registry.get_sample_value("jp_seismic_ingest_rows_rejected") == 2
    assert registry.get_sample_value("jp_seismic_ingest_duration_seconds") == 4.5
    assert registry.get_sample_value("jp_seismic_ingest_last_run_success") == 1
    assert registry.get_sample_value("jp_seismic_ingest_max_magnitude") == 6.4


def test_publish_ingest_run_omits_max_magnitude_on_a_quiet_window(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pushed: dict[str, CollectorRegistry] = {}
    monkeypatch.setattr(
        "jp_seismic.metrics.push_to_gateway",
        lambda url, *, job, registry, grouping_key: pushed.update(registry=registry),
    )

    publish_ingest_run(
        IngestRun(
            rows_read=0,
            rows_written=0,
            rows_rejected=0,
            duration_seconds=0.2,
            succeeded=True,
        ),
        MonitoringConfig(pushgateway_url="http://gw:9091", job_name="ingest"),
    )

    # Publishing a zero would read as "an M0 event happened" and would stick
    # around as the last known value until the next run overwrote it.
    assert (
        pushed["registry"].get_sample_value("jp_seismic_ingest_max_magnitude") is None
    )


def test_publish_ingest_run_omits_success_timestamp_when_the_run_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pushed: dict[str, CollectorRegistry] = {}
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


def test_publish_ingest_run_survives_an_unreachable_gateway(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    run: IngestRun,
) -> None:
    def unreachable(*_args: object, **_kwargs: object) -> None:
        raise OSError("connection refused")

    monkeypatch.setattr("jp_seismic.metrics.push_to_gateway", unreachable)

    publish_ingest_run(
        run, MonitoringConfig(pushgateway_url="http://gw:9091", job_name="ingest")
    )

    assert "could not push metrics" in caplog.text


def test_monitoring_config_treats_an_empty_url_as_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("PUSHGATEWAY_URL", "   ")

    assert MonitoringConfig.from_env().pushgateway_url is None
