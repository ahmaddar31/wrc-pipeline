"""Airflow DAG: WRC ingestion → transformation.

Weekly schedule. Dates are computed automatically:
  - end_date   = the day the DAG runs  ({{ ds }})
  - start_date = one week before       ({{ macros.ds_add(ds, -7) }})

Manual trigger with optional JSON config:
    {
        "start_date": "2024-01-01",
        "end_date":   "2024-03-31"
    }

Task order:
    scrape_landing_zone >> transform_processed_zone

Credentials are managed through Airflow Connections (Admin → Connections in the UI):
  - mongo_wrc  : MongoDB URI
  - minio_wrc  : MinIO host/port/login/password
These are injected as environment variables into each task at runtime, so the
app code continues reading from os.getenv() without any changes.
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.utils.email import send_email

_log = logging.getLogger("airflow.task")

_ALERT_EMAIL = os.getenv("AIRFLOW_ALERT_EMAIL", "")


def _on_failure(context):
    ti = context["task_instance"]
    _log.error(
        "Task failed | dag=%s | task=%s | run_id=%s | try_number=%s",
        ti.dag_id, ti.task_id, ti.run_id, ti.try_number,
    )
    if _ALERT_EMAIL:
        send_email(
            to=_ALERT_EMAIL,
            subject=f"[Airflow] FAILED: {ti.dag_id} › {ti.task_id}",
            html_content=(
                f"<b>DAG:</b> {ti.dag_id}<br>"
                f"<b>Task:</b> {ti.task_id}<br>"
                f"<b>Run ID:</b> {ti.run_id}<br>"
                f"<b>Try number:</b> {ti.try_number}<br>"
                f"<b>Log URL:</b> <a href='{ti.log_url}'>{ti.log_url}</a>"
            ),
        )


def _on_retry(context):
    ti = context["task_instance"]
    _log.warning(
        "Task retrying | dag=%s | task=%s | run_id=%s | try_number=%s",
        ti.dag_id, ti.task_id, ti.run_id, ti.try_number,
    )


def _conn_env() -> dict:
    """Read credentials from Airflow Connections and return as env-var dict.

    The app code reads MONGO_URI / MINIO_* from os.getenv(), so we bridge
    Airflow Connections → environment variables here.  Falls back gracefully
    if the connections haven't been registered yet (e.g. first-time setup).
    """
    env = {}
    try:
        mongo = BaseHook.get_connection("mongo_wrc")
        env["MONGO_URI"] = (
            f"mongodb://{mongo.login}:{mongo.get_password()}"
            f"@{mongo.host}:{mongo.port}/"
        )
    except Exception:
        pass  # connection not registered; app will use the value from .env

    try:
        minio = BaseHook.get_connection("minio_wrc")
        env["MINIO_ENDPOINT"] = f"{minio.host}:{minio.port}"
        env["MINIO_ACCESS_KEY"] = minio.login
        env["MINIO_SECRET_KEY"] = minio.get_password()
    except Exception:
        pass  # connection not registered; app will use the value from .env

    return env


with DAG(
    dag_id="wrc_ingestion_transformation_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    tags=["scrapy", "mongo", "minio", "wrc"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=30),
        "execution_timeout": timedelta(hours=3),
        "on_failure_callback": _on_failure,
        "on_retry_callback": _on_retry,
        "email_on_failure": False,  # handled manually in _on_failure above
        "email_on_retry": False,
    },
) as dag:

    _start = "{{ dag_run.conf.get('start_date', macros.ds_add(ds, -7)) }}"
    _end   = "{{ dag_run.conf.get('end_date',   ds) }}"
    _creds = _conn_env()

    scrape = BashOperator(
        task_id="scrape_landing_zone",
        execution_timeout=timedelta(hours=2),
        env=_creds,
        bash_command=(
            f"cd /opt/airflow && "
            f"python -m scrapy crawl workplace_relations "
            f"-a start_date={_start} "
            f"-a end_date={_end}"
        ),
        doc_md=(
            "Scrapes WRC decisions for the date window into the **landing zone**.\n\n"
            "- Writes raw files (HTML/PDF) to MinIO `wrc-landing`\n"
            "- Writes metadata to MongoDB `landing_metadata`\n"
            "- Per-record failures are logged and skipped; only critical errors fail the task"
        ),
    )

    transform = BashOperator(
        task_id="transform_processed_zone",
        execution_timeout=timedelta(hours=1),
        env=_creds,
        bash_command=(
            f"cd /opt/airflow && "
            f"python -m app.transform.transform "
            f"--start-date {_start} "
            f"--end-date {_end}"
        ),
        doc_md=(
            "Cleans and transforms landing documents into the **processed zone**.\n\n"
            "- Extracts decision content from HTML, passes PDFs through unchanged\n"
            "- Uploads to MinIO `wrc-processed`\n"
            "- Upserts metadata to MongoDB `processed_metadata`"
        ),
    )

    scrape >> transform
