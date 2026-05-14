from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="ev_charging_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    description="End-to-end EV charging ETL pipeline",
    tags=["data-engineering", "aws", "airflow"],
) as dag:

    extract = BashOperator(
        task_id="extract_api_to_s3",
        bash_command="python /opt/airflow/scripts/extract_api_to_s3.py",
    )

    transform = BashOperator(
        task_id="transform_to_parquet",
        bash_command="python /opt/airflow/scripts/transform_to_parquet.py",
    )

    load = BashOperator(
        task_id="upload_to_supabase",
        bash_command="python /opt/airflow/scripts/upload_to_supabase_.py",
    )

    extract >> transform >> load