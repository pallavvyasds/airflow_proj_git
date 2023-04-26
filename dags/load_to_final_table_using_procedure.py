from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests 
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time
from utility import *

default_args = {
    'owner': 'pallav',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
     default_args = default_args,
     dag_id= 'load_to_final_table_using_procedure_v1',
     description= 'load_to_final_table_using_procedure',
     start_date = datetime(2023, 4, 16),
     schedule_interval=  '@daily'
) as dag:
    
    task1 = PythonOperator(
        task_id='load_data_to_final',
        python_callable= load_data_to_final,
        dag = dag
    )
    task1