"""
dags/dag_backfill.py — Manual backfill orchestration.

This DAG is intended to be triggered manually via the Airflow UI or CLI.
It accepts parameters `start_date` and `end_date` to define the backfill range.

Pipeline flow:
  backfill_data (Python module)
    → dbt_silver → test_silver
      → dbt_gold → test_gold

Design decisions:
  - BashOperator only, matching the architecture of dag_daily.py.
  - Passes Airflow params to the Python CLI via Jinja templating.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.models.param import Param

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
RUN_PREFIX = f"cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR}"
DBT_PREFIX = f"cd {DBT_DIR} && PYTHONPATH={PROJECT_DIR}"

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-team",
    "retries": 0,
    "execution_timeout": timedelta(hours=2),  # Backfills can take longer
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="manual_backfill_pipeline",
    default_args=default_args,
    description="Manual backfill: Bronze → dbt Silver → dbt Gold",
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["stock", "backfill", "manual"],
    max_active_runs=1,
    params={
        "start_date": Param("2021-01-01", type="string", format="date", description="Start date (YYYY-MM-DD)"),
        "end_date": Param("2026-06-18", type="string", format="date", description="End date (YYYY-MM-DD)"),
    },
) as dag:

    # 1. Run backfill script
    backfill_data = BashOperator(
        task_id="backfill_data",
        bash_command=(
            f"{RUN_PREFIX} python -m ingestion.backfill "
            "--start {{ params.start_date }} --end {{ params.end_date }}"
        ),
        execution_timeout=timedelta(hours=1),
    )

    # 2. dbt run — Silver
    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"{DBT_PREFIX} dbt run "
            "--select silver "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # 3. dbt test — Silver
    test_silver = BashOperator(
        task_id="dbt_test_silver",
        bash_command=(
            f"{DBT_PREFIX} dbt test "
            "--select silver "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # 4. dbt run — Gold
    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"{DBT_PREFIX} dbt run "
            "--select gold "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=20),
    )

    # 5. dbt test — Gold
    test_gold = BashOperator(
        task_id="dbt_test_gold",
        bash_command=(
            f"{DBT_PREFIX} dbt test "
            "--select gold "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=5),
    )

    # ---------------------------------------------------------------------------
    # Task dependencies
    # ---------------------------------------------------------------------------
    backfill_data >> dbt_silver >> test_silver >> dbt_gold >> test_gold
