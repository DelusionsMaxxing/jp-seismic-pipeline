"""Prometheus metrics describing a single ingest run.

Ingest is a short-lived batch job: it exits long before Prometheus would get
round to scraping it, so a run's numbers are pushed to a Pushgateway instead
of being exposed on a port. The push is optional — with ``PUSHGATEWAY_URL``
unset every helper here is a no-op, so tests, CI and a laptop run behave
exactly as they did before the monitoring stack existed.
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from .config import MonitoringConfig

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class IngestRun:
    """The four numbers every run reports, plus whether it finished."""

    rows_read: int
    rows_written: int
    rows_rejected: int
    duration_seconds: float
    succeeded: bool


def publish_ingest_run(
    run: IngestRun,
    config: MonitoringConfig,
    source: str = "usgs",
) -> None:
    """Push one run's metrics to the Pushgateway, or do nothing if unconfigured.

    ``source`` becomes a grouping key so a later run replaces the previous
    values rather than accumulating a new series per run — the gateway holds
    the last known state of each job, not a history.
    """
    if config.pushgateway_url is None:
        logger.debug("no pushgateway configured; skipping metric push")
        return

    registry = CollectorRegistry()

    def gauge(name: str, documentation: str, value: float) -> None:
        Gauge(name, documentation, registry=registry).set(value)

    gauge(
        "jp_seismic_ingest_rows_read",
        "Events returned by the source API during the last run.",
        run.rows_read,
    )
    gauge(
        "jp_seismic_ingest_rows_written",
        "Rows upserted into raw.earthquake_events during the last run.",
        run.rows_written,
    )
    gauge(
        "jp_seismic_ingest_rows_rejected",
        "Features discarded during the last run because they carried no id.",
        run.rows_rejected,
    )
    gauge(
        "jp_seismic_ingest_duration_seconds",
        "Wall-clock duration of the last run.",
        run.duration_seconds,
    )
    gauge(
        "jp_seismic_ingest_last_run_success",
        "1 if the last run completed, 0 if it raised.",
        float(run.succeeded),
    )
    if run.succeeded:
        # Only advanced on success, so staleness alerts measure time since the
        # last *good* run rather than time since the last attempt.
        gauge(
            "jp_seismic_ingest_last_success_timestamp_seconds",
            "Unix time at which the last successful run finished.",
            time.time(),
        )

    try:
        push_to_gateway(
            config.pushgateway_url,
            job=config.job_name,
            registry=registry,
            grouping_key={"source": source},
        )
    except OSError as exc:
        # Monitoring must never take the pipeline down with it: a gateway that
        # is unreachable is a monitoring outage, not an ingest failure.
        logger.warning("could not push metrics to %s: %s", config.pushgateway_url, exc)
    else:
        logger.info(
            "pushed run metrics: read=%d written=%d rejected=%d duration=%.1fs",
            run.rows_read,
            run.rows_written,
            run.rows_rejected,
            run.duration_seconds,
        )
