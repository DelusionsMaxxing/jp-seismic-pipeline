from __future__ import annotations

from pathlib import Path

import pytest

# Airflow is not a dependency of the package — it is supplied by the base image
# in the container and installed explicitly in CI. Locally the suite skips
# rather than forcing a 200-package install on anyone running unit tests.
pytest.importorskip("airflow", reason="apache-airflow is not installed")

from airflow.models import DagBag  # noqa: E402  (must follow importorskip)

pytestmark = pytest.mark.airflow

DAGS_DIR = Path(__file__).resolve().parent.parent / "dags"


@pytest.fixture(scope="module")
def dagbag() -> DagBag:
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dagbag_has_no_import_errors(dagbag: DagBag) -> None:
    assert dagbag.import_errors == {}


def test_daily_dag_is_registered(dagbag: DagBag) -> None:
    assert "jp_seismic_daily" in dagbag.dags


def test_every_task_declares_an_execution_timeout(dagbag: DagBag) -> None:
    # A task without one holds its worker slot forever when it hangs, and no
    # amount of retry configuration recovers from that.
    missing = [
        f"{dag_id}.{task.task_id}"
        for dag_id, dag in dagbag.dags.items()
        for task in dag.tasks
        if task.execution_timeout is None
    ]

    assert missing == []


def test_dbt_is_one_task_per_layer_in_dependency_order(dagbag: DagBag) -> None:
    # A single dbt task names neither the layer nor the model when it fails,
    # and building every layer before testing any of them means a broken
    # staging model is only caught after the marts are built on top of it.
    dag = dagbag.dags["jp_seismic_daily"]

    assert [task.task_id for task in dag.topological_sort()] == [
        "ingest_events",
        "build_staging_models",
        "build_intermediate_models",
        "build_marts_models",
    ]


@pytest.mark.parametrize("layer", ["staging", "intermediate", "marts"])
def test_dbt_task_builds_only_its_own_layer(dagbag: DagBag, layer: str) -> None:
    task = dagbag.dags["jp_seismic_daily"].get_task(f"build_{layer}_models")

    assert f"dbt build --target dev --select path:models/{layer}" in task.bash_command


def test_every_dag_has_an_owner_and_retries(dagbag: DagBag) -> None:
    for dag_id, dag in dagbag.dags.items():
        assert dag.default_args.get("owner"), f"{dag_id} has no owner"
        assert dag.default_args.get("retries", 0) >= 1, f"{dag_id} has no retries"
