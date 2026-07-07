"""
dags/dag_daily.py — Daily stock data pipeline orchestration.

Schedule: 18:00 UTC+7 (11:00 UTC) every weekday — after HOSE closes at 15:00.

Pipeline flow (sequential):
  health_check
    → fetch_prices_vn30   (always — VN30 dynamic from API)
    → fetch_prices_others (mặc định bật — ~373 mã HOSE còn lại, skip nếu run_vn30_only=True)
    → fetch_index         (song song với fetch_prices_vn30)
      → dbt_silver → test_silver
        → dbt_gold → test_gold
          → notify_success

Default behaviour: run ALL HOSE stocks (VN30 first, then ~373 others).
Set run_vn30_only=True at trigger time to run only VN30 (faster, ~5 min).

Design decisions:
  - BashOperator only — no PythonOperator — because ingestion/ and dbt/
    live on the host and are mounted into /opt/airflow/project.
  - DB_HOST inside container is 'db' (Docker service name), not 'localhost'.
  - All env vars are set in docker-compose.yml environment block.
  - Each task cd's into /opt/airflow/project so PYTHONPATH resolves correctly.
  - Retry 3x with exponential backoff for fetch tasks (transient API errors).
  - on_failure_callback logs to Airflow's built-in alerting.
  - VN30 symbol list is DYNAMIC (from Listing API), not hardcoded.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param
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
    execution_date = getattr(ti, "logical_date", getattr(ti, "execution_date", "Unknown"))
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
    params={
        # Mặc định: chạy TOÀN BỘ HOSE (VN30 trước, ~373 mã còn lại sau).
        # Bật run_vn30_only=True khi cần chạy nhanh chỉ 30 mã VN30 (~5 phút).
        "run_vn30_only": Param(False, type="boolean",
                               description="Chỉ kéo VN30, bỏ qua ~373 mã HOSE còn lại"),
    },
) as dag:

    # 1. Health check — verify API connectivity
    health_check = BashOperator(
        task_id="health_check",
        bash_command=(
            f'{RUN_PREFIX} python -c "'
            "import sys; "
            "from providers.registry import get_provider; "
            "p = get_provider(); "
            "sys.exit(0 if p.health_check() else 1)"
            '"'
        ),
        execution_timeout=timedelta(minutes=2),
    )

    # 2a. Fetch stock prices (VN30) — today only
    # VN30 LUÔN chạy — danh sách 30 mã được lấy động từ Listing API, không hardcode.
    fetch_prices_vn30 = BashOperator(
        task_id="fetch_prices_vn30",
        bash_command=(
            "{% set d = dag_run.logical_date.strftime('%Y-%m-%d') if (dag_run and dag_run.logical_date) else macros.datetime.now().strftime('%Y-%m-%d') %}"
            f"{RUN_PREFIX} python -m ingestion.fetch_prices --mode vn30 --start {{{{ d }}}} --end {{{{ d }}}}"
        ),
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=30),
    )

    # 2b. Fetch stock prices (HOSE Others) — today only
    # Mặc định BẬT. Chỉ skip khi run_vn30_only=True.
    # ~373 mã = tất cả HOSE STOCK (403) trừ 30 VN30, lấy động từ API.
    fetch_prices_others = BashOperator(
        task_id="fetch_prices_others",
        bash_command=(
            "{% set d = dag_run.logical_date.strftime('%Y-%m-%d') if (dag_run and dag_run.logical_date) else macros.datetime.now().strftime('%Y-%m-%d') %}"
            "{% if not params.run_vn30_only %}"
            f"{RUN_PREFIX} python -m ingestion.fetch_prices --mode others --start {{{{ d }}}} --end {{{{ d }}}}"
            "{% else %}"
            'echo "[SKIP] fetch_prices_others: run_vn30_only=true" && exit 99'
            "{% endif %}"
        ),
        skip_on_exit_code=99,
        retries=3,
        retry_delay=timedelta(minutes=2),
        retry_exponential_backoff=True,
        execution_timeout=timedelta(minutes=60),
    )

    # 3. Fetch index prices (VNINDEX, VN30)
    fetch_index = BashOperator(
        task_id="fetch_index",
        bash_command=(
            f"{RUN_PREFIX} python -m ingestion.fetch_index "
            "{% set d = dag_run.logical_date.strftime('%Y-%m-%d') if (dag_run and dag_run.logical_date) else macros.datetime.now().strftime('%Y-%m-%d') %}"
            "--start {{ d }} --end {{ d }}"
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
        trigger_rule="none_failed_min_one_success",
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
    # health_check → [fetch_prices_vn30 (sequential → others), fetch_index]
    # VN30 chạy trước (task 2a), khi xong mới chạy others (task 2b)
    # fetch_index chạy song song với nhóm fetch_prices
    # [fetch_prices_others, fetch_index] >> dbt_silver >> ... >> notify_success

    health_check >> [fetch_prices_vn30, fetch_index]
    fetch_prices_vn30 >> fetch_prices_others
    [fetch_prices_others, fetch_index] >> dbt_silver >> test_silver
    test_silver >> dbt_gold >> test_gold >> notify_success
