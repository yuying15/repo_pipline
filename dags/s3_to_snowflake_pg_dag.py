from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.excel_to_snowflake_etl import excel_to_snowflake_etl
from etl.new_task import new_task_function
from ETL_S3_SNOWFLAKES import execute_snowflake_sql
from etl.sf_to_postgresql import sf_to_postgresql
from dotenv import load_dotenv
load_dotenv()
with DAG(
    dag_id="s3_snowflake_postgresql",
    start_date=datetime(2023, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:

    execute_sql_task = PythonOperator(
        task_id="execute_snowflake_sql",
        python_callable=execute_snowflake_sql
    )

    sf_to_pg_task = PythonOperator(
        task_id="sf_to_postgresql",
        python_callable=sf_to_postgresql
    )

    # Set task dependencies

    execute_sql_task >> sf_to_pg_task
