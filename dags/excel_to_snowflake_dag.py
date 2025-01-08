from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

def run_script(script_path):
    subprocess.run(["python", script_path], check=True)

with DAG(
    dag_id="excel_to_snowflake",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    etl_task = PythonOperator(
        task_id="excel_to_snowflake_etl",
        python_callable=run_script,
        op_kwargs={
            "script_path": "/opt/airflow/dags/etl/excel_to_snowflake_etl.py"
        }
    )

    new_task = PythonOperator(
        task_id="new_task",
        python_callable=run_script,
        op_kwargs={
            "script_path": "/opt/airflow/dags/etl/new_task.py"
        }
    )

    execute_sql_task = PythonOperator(
        task_id="execute_snowflake_sql",
        python_callable=run_script,
        op_kwargs={
            "script_path": "/opt/airflow/dags/etl/ETL_S3_SNOWFLAKES.py"
        }
    )

    sf_to_pg_task = PythonOperator(
        task_id="sf_to_postgresql",
        python_callable=run_script,
        op_kwargs={
            "script_path": "/opt/airflow/dags/etl/sf_to_postgresql.py"
        }
    )

    # Set task dependencies
    etl_task >> new_task
    new_task >> execute_sql_task
    execute_sql_task >> sf_to_pg_task
