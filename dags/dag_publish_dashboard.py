"""
dags/dag_publish_dashboard.py — Automatically generate dashboard index.html and push to GitHub Pages.

Schedule: 18:20 UTC+7 (11:20 UTC) every weekday.
"""

import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/opt/airflow/project"
RUN_PREFIX = f"cd {PROJECT_DIR} && PYTHONPATH={PROJECT_DIR}"

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=10),
}

with DAG(
    dag_id="publish_dashboard_pipeline",
    default_args=default_args,
    description="Daily: Generate HTML dashboard & Push to GitHub Pages",
    schedule="20 11 * * 1-5",  # 18:20 Việt Nam (11:20 UTC), từ Thứ 2 đến Thứ 6
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=["dashboard", "publish"],
) as dag:

    # 1. Sinh file HTML từ dữ liệu Gold mới nhất
    generate_html = BashOperator(
        task_id="generate_html",
        bash_command=f"{RUN_PREFIX} python scripts/generate_dashboard_backup.py",
    )

    # 2. Thực hiện git commit và git push sử dụng PAT
    git_push_github = BashOperator(
        task_id="git_push_github",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            f"git config --global --add safe.directory {PROJECT_DIR} && "
            f"git config user.name 'Airflow Bot' && "
            f"git config user.email 'airflow-bot@example.com' && "
            f"git add docs/index.html && "
            f"(git diff-index --quiet HEAD || git commit -m 'auto-update: dashboard data $(date +\"%Y-%m-%d %H:%M:%S\")') && "
            f"git push https://${{GITHUB_PAT}}@${{GITHUB_REPO}} main"
        ),
        env={
            "GITHUB_PAT": os.getenv("GITHUB_PAT", ""),
            "GITHUB_REPO": os.getenv("GITHUB_REPO", ""),
        }
    )

    generate_html >> git_push_github
