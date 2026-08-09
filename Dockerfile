FROM apache/airflow:2.10.3-python3.11

USER root
RUN apt-get update \
    && apt-get install --no-install-recommends -y curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Install the pipeline package itself so the DAG can import jp_seismic.
COPY --chown=airflow:root pyproject.toml /opt/airflow/project/pyproject.toml
COPY --chown=airflow:root src /opt/airflow/project/src
RUN pip install --no-cache-dir -e /opt/airflow/project

# Vendor dbt packages at build time so the scheduler never needs network
# access to run a transformation.
COPY --chown=airflow:root dbt/packages.yml /opt/airflow/dbt/packages.yml
COPY --chown=airflow:root dbt/dbt_project.yml /opt/airflow/dbt/dbt_project.yml

WORKDIR /opt/airflow/dbt
RUN dbt deps
WORKDIR /opt/airflow
