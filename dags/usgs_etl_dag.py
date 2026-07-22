"""
Bonus: local-runnable Airflow DAG that wraps the same etl.py stages.

Reuses the functions in etl.py so the DAG and CLI share one implementation.
Run locally with:

    export AIRFLOW_HOME=~/airflow
    export GOOGLE_CLOUD_PROJECT=your-project
    airflow standalone            # first-time: creates admin user
    # copy this file into $AIRFLOW_HOME/dags/ and add the repo root to PYTHONPATH
    airflow dags test usgs_earthquakes_etl 2018-05-31
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys

# Make the repo root importable so we can reuse etl.py.
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from airflow.decorators import dag, task  # noqa: E402
from google.cloud import bigquery  # noqa: E402

import etl  # noqa: E402

PROJECT = os.environ.get("GOOGLE_CLOUD_PROJECT", "your-project-id")


def _args(start: dt.date, end: dt.date) -> argparse.Namespace:
    """Build the same args object etl.py's functions expect (fixed scope defaults)."""
    return argparse.Namespace(
        project=PROJECT,
        start_date=start.isoformat(),
        end_date=end.isoformat(),
        min_magnitude=3.0,
        min_latitude=32.0, max_latitude=42.0,
        min_longitude=-125.0, max_longitude=-114.0,
        page_limit=20000,
    )


@dag(
    dag_id="usgs_earthquakes_etl",
    schedule="@daily",
    start_date=dt.datetime(2018, 5, 1, tzinfo=dt.timezone.utc),
    end_date=dt.datetime(2018, 5, 31, tzinfo=dt.timezone.utc),
    catchup=False,            # set True to let Airflow backfill each daily partition
    default_args={"owner": "data-eng", "retries": 2,
                  "retry_delay": dt.timedelta(minutes=2)},
    tags=["usgs", "bigquery", "etl"],
)
def usgs_etl():
    """Two idempotent stages: ingest+merge, then build the model."""

    @task
    def ingest(data_interval_start=None, data_interval_end=None) -> int:
        args = _args(data_interval_start.date(), data_interval_end.date())
        rows = list(etl.fetch(args))
        if rows:
            etl.load_raw(bigquery.Client(project=PROJECT), args, rows)
        return len(rows)

    @task
    def model(_count: int, data_interval_start=None, data_interval_end=None) -> None:
        args = _args(data_interval_start.date(), data_interval_end.date())
        etl.build_model(bigquery.Client(project=PROJECT), args)

    model(ingest())


dag_instance = usgs_etl()
