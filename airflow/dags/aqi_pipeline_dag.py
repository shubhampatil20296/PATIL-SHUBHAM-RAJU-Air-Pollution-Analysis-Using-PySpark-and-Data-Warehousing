from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

PROJECT_DIR = "/home/sunbeam/Downloads/BD_Pro_draft"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="aqi_end_to_end_pipeline",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule=None,          # ✅ Airflow 3.x way
    catchup=False,
    tags=["aqi", "spark", "ml", "medallion"],
) as dag:

    bronze = BashOperator(
        task_id="bronze_ingestion",
        bash_command=f"python3 {PROJECT_DIR}/bronze/data.py"
    )

    silver = BashOperator(
        task_id="silver_cleaning",
        bash_command=f"python3 {PROJECT_DIR}/silver/spark.py"
    )

    gold = BashOperator(
        task_id="gold_hive_load",
        bash_command=f"python3 {PROJECT_DIR}/gold/hive.py"
    )

    ml = BashOperator(
        task_id="ml_clustering",
        bash_command=f"python3 {PROJECT_DIR}/ml/ml.py"
    )

    bronze >> silver >> gold >> ml
