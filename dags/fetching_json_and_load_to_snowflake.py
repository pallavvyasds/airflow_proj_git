from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests 
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from utility import *

default_args = {
    'owner': 'pallav',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}
   
with DAG(
     default_args = default_args,
     dag_id= 'fetching_json_and_load_to_snowflake_v13',
     description= 'fetching_json_and_load_to_snowflake',
     start_date = datetime(2023, 4, 16),
     schedule_interval=  '@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id='fetch_data_from_api',
        python_callable= fetch_data_from_api,
        # passing api_url in fetch_data_from_api function
        op_kwargs={'api_url': 'http://api.coincap.io/v2/assets'},
        dag = dag
    )
    task2 = PythonOperator(
        task_id= 'local_to_json_stage',
        python_callable= local_to_json_stage,
        dag = dag
    )
    task3 = PythonOperator(
        task_id= 'stage_to_table',
        python_callable= stage_to_table,
        dag = dag
    )

    task4 = TriggerDagRunOperator(
        task_id="trigger_load_to_final_table_using_procedure_DAG",
        trigger_dag_id="load_to_final_table_using_procedure_v1",
        wait_for_completion=True
    )
   
    task1 >> task2 >> task3 >> task4