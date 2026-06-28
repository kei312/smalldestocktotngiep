"""
dags/dag_backfill.py — Manual backfill orchestration.

This DAG is intended to be triggered manually via the Airflow UI or CLI.
It accepts parameters `start_date`, `end_date` and `run_vn30_only` to define
the backfill range and scope.

Pipeline flow (sequential):
  health_check
    → backfill_vn30    (always — 30 VN30 symbols, dynamic from API)
    → backfill_others  (default ON — ~373 remaining HOSE stocks, skip if run_vn30_only=True)
      → dbt_silver → test_silver
        → dbt_gold → test_gold

Default behaviour: backfill ALL HOSE stocks (VN30 first, then ~373 others).
Set run_vn30_only=True to backfill only VN30 (faster, useful for demos/checks).

Design decisions:
  - BashOperator only, matching the architecture of dag_daily.py.
  - Passes Airflow params to the Python CLI via Jinja templating.
  - VN30 list is DYNAMIC (Listing API), not hardcoded in config.
  - Two separate tasks allow partial retry without re-fetching VN30.
"""

from datetime import datetime, timedelta

from airflow.sdk import DAG, Param
from airflow.providers.standard.operators.bash import BashOperator

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
        "start_date": Param("2021-01-01", type="string", format="date",
                            description="Start date (YYYY-MM-DD)"),
        "end_date":   Param("2026-06-24", type="string", format="date",
                            description="End date (YYYY-MM-DD)"),
        # Mặc định: backfill TOÀN BỘ HOSE (VN30 trước, ~373 mã còn lại sau).
        # Bật run_vn30_only=True để chỉ backfill 30 mã VN30 (nhanh hơn).
        "run_vn30_only": Param(False, type="boolean",
                               description="Chỉ backfill VN30, bỏ qua ~373 mã HOSE còn lại"),
    },
) as dag:

    # 0. Health check — verify API connectivity
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

    # 1a. Backfill VN30 — LUÔN chạy
    # Danh sách 30 mã lấy động từ Listing API (symbols_by_group VN30).
    backfill_vn30 = BashOperator(
        task_id="backfill_vn30",
        bash_command=(
            f"{RUN_PREFIX} python -m ingestion.backfill "
            "--start {{ params.start_date }} --end {{ params.end_date }} "
            "--mode vn30"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=2),
    )

    # 1b. Backfill HOSE Others — mặc định BẬT, skip nếu run_vn30_only=True
    # ~373 mã = 403 HOSE STOCK (từ API) trừ 30 VN30.
    backfill_others = BashOperator(
        task_id="backfill_others",
        bash_command=(
            "{% if not params.run_vn30_only %}"
            f"{RUN_PREFIX} python -m ingestion.backfill "
            "--start {{ params.start_date }} --end {{ params.end_date }} "
            "--mode others"
            "{% else %}"
            'echo "[SKIP] backfill_others: run_vn30_only=true"'
            "{% endif %}"
        ),
        retries=2,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(hours=4),
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
    # VN30 chạy trước → xong mới chạy others → rồi mới vào dbt layer
    # Cho phép retry backfill_others độc lập nếu VN30 đã thành công.
    health_check >> backfill_vn30 >> backfill_others >> dbt_silver >> test_silver >> dbt_gold >> test_gold
