"""
dags/dag_daily.py — Daily stock data pipeline orchestration.

Schedule: 18:00 UTC+7 (11:00 UTC) every weekday — after HOSE closes at 15:00.

Pipeline flow:
  health_check → fetch_prices → fetch_index
    → dbt_silver → test_silver
      → dbt_gold → test_gold
        → notify_success

Design decisions:
  - BashOperator only — no PythonOperator — because ingestion/ and dbt/
    live on the host and are mounted into /opt/airflow/project.
  - DB_HOST inside container is 'db' (Docker service name), not 'localhost'.
  - All env vars are set in docker-compose.yml environment block.
  - Each task cd's into /opt/airflow/project so PYTHONPATH resolves correctly.
  - Retry 3x with exponential backoff for fetch tasks (transient API errors).
  - on_failure_callback logs to Airflow's built-in alerting.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
PROJECT_DIR = "/opt/airflow/project"
DBT_DIR = f"{PROJECT_DIR}/dbt"
# Shared prefix: cd into project so that `python -m ingestion.*` works
RUN_PREFIX = f"cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR}"
# dbt prefix: cd into dbt/ so profiles.yml is found
DBT_PREFIX = f"cd {DBT_DIR} && PYTHONPATH={PROJECT_DIR}"

# ---------------------------------------------------------------------------
# Failure callback
# ---------------------------------------------------------------------------
def _on_failure(context):
    """Log task failure details to Airflow logger."""
    ti = context["task_instance"]
    dag_id = ti.dag_id
    task_id = ti.task_id
    execution_date = context["logical_date"]
    exception = context.get("exception", "Unknown")
    print(
        f"[ALERT] DAG={dag_id} Task={task_id} "
        f"Date={execution_date} Error={exception}"
    )

# ---------------------------------------------------------------------------
# Default args
# ---------------------------------------------------------------------------
default_args = {
    "owner": "data-team",
    "retries": 0,                                # default: no retry
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=30),
    "on_failure_callback": _on_failure,
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="daily_stock_pipeline",
    default_args=default_args,
    description="Daily: fetch OHLCV → Bronze → dbt Silver → dbt Gold",
    schedule="0 11 * * 1-5",  # 18:00 VN = 11:00 UTC, Mon-Fri
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["stock", "daily", "production"],
    max_active_runs=1,
) as dag:

    # 1. Health check — verify API connectivity
    health_check = BashOperator(
        task_id="health_check",
        bash_command=(
            f'{RUN_PREFIX} python -c "'
            "from providers.registry import get_provider; "
            "p = get_provider(); "
            "p.health_check(); "
            "print('[OK] Provider healthy')"
            '"'
        ),
        execution_timeout=timedelta(minutes=2),
    )

    # 2. Fetch stock prices (VN30) — today only
    fetch_prices = BashOperator(
        task_id="fetch_prices",
        bash_command=(
            f"{RUN_PREFIX} python -m ingestion.fetch_prices "
            "--start {{ ds }} --end {{ ds }} --skip-index"
        ),
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=15),
    )

    # 3. Fetch index prices (VNINDEX, VN30)
    fetch_index = BashOperator(
        task_id="fetch_index",
        bash_command=(
            f"{RUN_PREFIX} python -m ingestion.fetch_prices "
            "--start {{ ds }} --end {{ ds }} --skip-prices"
        ),
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=10),
    )

    # 4. dbt run — Silver layer (cleaning + validation)
    dbt_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command=(
            f"{DBT_PREFIX} dbt run "
            "--select silver "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=10),
    )

    # 5. dbt test — Silver quality gates
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

    # 6. dbt run — Gold layer (indicators + star schema)
    dbt_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command=(
            f"{DBT_PREFIX} dbt run "
            "--select gold "
            "--profiles-dir . "
            "--project-dir ."
        ),
        execution_timeout=timedelta(minutes=15),
    )

    # 7. dbt test — Gold quality gates (RSI range, BB, MACD accuracy)
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

    # 8. Notify success
    notify_success = BashOperator(
        task_id="notify_success",
        bash_command=(
            'echo "[SUCCESS] Daily pipeline completed at $(date +%Y-%m-%dT%H:%M:%S)"'
        ),
    )

    # ---------------------------------------------------------------------------
    # Task dependencies — linear pipeline
    # ---------------------------------------------------------------------------
    # health_check → [fetch_prices, fetch_index] → dbt_silver → test_silver
    #   → dbt_gold → test_gold → notify_success

    health_check >> [fetch_prices, fetch_index]
    [fetch_prices, fetch_index] >> dbt_silver >> test_silver
    test_silver >> dbt_gold >> test_gold >> notify_success
