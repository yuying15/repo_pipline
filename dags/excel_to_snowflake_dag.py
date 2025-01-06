from airflow import DAG 
from airflow.operators.python import PythonOperator 
from datetime import datetime 
from etl.excel_to_snowflake_etl import excel_to_snowflake_etl 
 
with DAG( 
    dag_id="excel_to_snowflake", 
    start_date=datetime(2023, 1, 1), 
    schedule_interval="@daily", 
    catchup=False 
) as dag: 
    etl_task = PythonOperator( 
        task_id="excel_to_snowflake_etl", 
        python_callable=excel_to_snowflake_etl, 
        op_kwargs={ 
            "excel_path": "/opt/airflow/excel/AdventureWorks_Sales.xlsx", 
            "target_table": "target_table_name" 
        } 
    ) 
