"""Prometheus metrics describing a single ingest run."""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from prometheus_client import CollectorRegistry, Gauge, push_to_gateway

from .config import MonitoringConfig

logger = logging.getLogger(__name__)

METRIC_PREFIX = "jp_seismic_ingest_"


@dataclass(frozen=True, slots=True)
class IngestRun:
    rows_read: int
    rows_written: int
    rows_rejected: int
    duration_seconds: float
    succeeded: bool
    # None on a window that held no event carrying a magnitude - a quiet day,
    # not a failure. Publishing 0.0 instead would read as an M0 event.
    max_magnitude: float | None = None


def _registry_for(run: IngestRun) -> CollectorRegistry:
    registry = CollectorRegistry()

    def gauge(name: str, documentation: str, value: float) -> None:
        Gauge(METRIC_PREFIX + name, documentation, registry=registry).set(value)

    gauge(
        "rows_read",
        "Events returned by the source API during the last run.",
        run.rows_read,
    )
    gauge(
        "rows_written",
        "Rows upserted into raw.earthquake_events during the last run.",
        run.rows_written,
    )
    gauge(
        "rows_rejected",
        "Features discarded during the last run for carrying no id.",
        run.rows_rejected,
    )
    gauge(
        "duration_seconds",
        "Wall-clock duration of the last run.",
        run.duration_seconds,
    )
    gauge(
        "last_run_success",
        "1 if the last run completed, 0 if it raised.",
        float(run.succeeded),
    )

    if run.max_magnitude is not None:
        gauge(
            "max_magnitude",
            "Largest magnitude among the events loaded by the last run.",
            run.max_magnitude,
        )

    if run.succeeded:
        # Advanced only by a run that finished, so staleness alerts measure
        # time since the last good run rather than the last attempt.
        gauge(
            "last_success_timestamp_seconds",
            "Unix time at which the last successful run finished.",
            time.time(),
        )

    return registry


def publish_ingest_run(
    run: IngestRun,
    config: MonitoringConfig,
    source: str = "usgs",
) -> None:
    if config.pushgateway_url is None:
        logger.debug("no pushgateway configured; skipping metric push")
        return

    try:
        # source is a grouping key, so a later run replaces the previous values
        # instead of leaving a new series behind for every run ever made.
        push_to_gateway(
            config.pushgateway_url,
            job=config.job_name,
            registry=_registry_for(run),
            grouping_key={"source": source},
        )
    except OSError as exc:
        # A gateway that is down is a monitoring outage, not an ingest failure.
        logger.warning("could not push metrics to %s: %s", config.pushgateway_url, exc)
    else:
        logger.info(
            "pushed run metrics: read=%d written=%d rejected=%d duration=%.1fs",
            run.rows_read,
            run.rows_written,
            run.rows_rejected,
            run.duration_seconds,
        )
