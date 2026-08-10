"""Daily ELT: ingest Japanese seismic events, then transform them with dbt.

The DAG is written to be backfillable — every task derives its window from
the Airflow data interval rather than from wall-clock time, so
``airflow dags backfill`` reproduces history exactly.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
from airflow.decorators import dag, task
from airflow.models.baseoperator import chain
from airflow.operators.bash import BashOperator

DBT_DIR = os.environ.get("DBT_PROJECT_DIR", "/opt/airflow/dbt")

# The dbt layers, in dependency order. One task each, so a red task in the grid
# names the layer that broke instead of "the transformation failed".
DBT_LAYERS = ("staging", "intermediate", "marts")

# dbt writes compiled artefacts into the project dir, which the container
# cannot write to when it is bind-mounted from the host. DBT_TARGET_PATH and
# DBT_LOG_PATH redirect them, and come from the container environment via
# append_env below rather than being repeated here.
DBT_ENV = {
    "DBT_PROFILES_DIR": DBT_DIR,
    "POSTGRES_HOST": os.environ.get("POSTGRES_HOST", "postgres"),
    "POSTGRES_PORT": os.environ.get("POSTGRES_PORT", "5432"),
    "POSTGRES_USER": os.environ.get("POSTGRES_USER", "seismic"),
    "POSTGRES_PASSWORD": os.environ.get("POSTGRES_PASSWORD", "seismic"),
    "POSTGRES_DB": os.environ.get("POSTGRES_DB", "seismic"),
}

default_args = {
    "owner": "data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # A day's ingest and a full dbt build both finish in seconds; 15 minutes is
    # slack for a slow USGS response, not a plausible runtime. Without it a task
    # that hangs on a socket holds its slot until somebody notices by hand.
    "execution_timeout": timedelta(minutes=15),
}


def build_layer(layer: str) -> BashOperator:
    """Build one dbt layer and run its tests before the next layer starts."""
    return BashOperator(
        task_id=f"build_{layer}_models",
        bash_command=(
            f"cd {DBT_DIR} && dbt build --target dev --select path:models/{layer}"
        ),
        env=DBT_ENV,
        append_env=True,
    )


@dag(
    dag_id="jp_seismic_daily",
    description="Ingest USGS seismic events for Japan and model them with dbt",
    schedule="0 3 * * *",
    # Late enough that the previous UTC day is closed and USGS has published
    # reviewed solutions for most of it.
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    # Bounds the whole run, including time spent waiting between retries, so a
    # run cannot stay alive across the next day's schedule.
    dagrun_timeout=timedelta(hours=1),
    default_args=default_args,
    tags=["seismic", "japan", "elt", "dbt"],
    doc_md=__doc__,
)
def jp_seismic_daily() -> None:
    @task(task_id="ingest_events")
    def ingest_events(data_interval_start=None, data_interval_end=None) -> int:
        """Load one interval's worth of raw events into ``raw.earthquake_events``."""
        from jp_seismic.cli import ingest

        return ingest(
            start=data_interval_start.date(),
            end=data_interval_end.date(),
        )

    # `build` rather than `run` then `test`: each layer's tests run against the
    # layer that just built, so a broken staging assumption stops the run before
    # the marts are built on top of it.
    chain(ingest_events(), *[build_layer(layer) for layer in DBT_LAYERS])


jp_seismic_daily()
