docker exec airflow-container airflow dags trigger manual_backfill_pipeline --conf '{"start_date": "2026-06-20", "end_date": "2026-06-23", "mode": "vn30"}'
