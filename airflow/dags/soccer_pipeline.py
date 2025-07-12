from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'soccer_pipeline',
    default_args=default_args,
    description='End-to-end soccer data pipeline',
    schedule_interval='@daily',
)

def run_etl():
    from etl.extract_load import load_to_postgres
    load_to_postgres()

def run_dbt():
    import subprocess
    subprocess.run(["dbt", "run"], cwd="dbt/")

etl_task = PythonOperator(
    task_id='extract_and_load',
    python_callable=run_etl,
    dag=dag,
)

dbt_task = PythonOperator(
    task_id='run_dbt_transformations',
    python_callable=run_dbt,
    dag=dag,
)

etl_task >> dbt_task