from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests 
import json
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import time
import os
from dotenv import load_dotenv
load_dotenv()

conn = snowflake.connector.connect(
        user = os.getenv('SNOWFLAKE_USER'),
        password= os.getenv('SNOWFLAKE_PASSWORD'),
        account= os.getenv('SNOWFLAKE_ACCOUNT'),
        database= os.getenv('SNOWFLAKE_DATABASE'),
        schema= os.getenv('SNOWFLAKE_SCHEMA')
)

def execute_snowflake_commands(executable_command):
    """ This Method is used to execute all SQL commands in snowflake
    Args: 
        - executable_command: This parameter will get sql commands and execute them in our snowflake database
    Returns:
        - This method returns nothing
    """
    try:
        conn.cursor().execute(executable_command)
    except Exception as e:
        print('Connection to snowflake failed, Exception: ', e)

def fetch_data_from_api(api_url): 
    """ This Method will fetch data from local api and store that in docker container in a defined path
    Args: 
        - api_url: This parameter will get url of open api from which we can fetch our data
    Returns:
        - This method returns nothing
    """
    url = api_url
    headers = {
        'Accept' : 'application/json',
        'Content-Type' : 'application/json'
    }
    
    try:
        response = requests.request("GET", url, headers=headers, data={})
        myjson = response.json()
    except Exception as e:
        print('Unable to load data from url, Exception: ', e)
    with open(os.getenv('PATH_OF_DATAFILE'), 'w') as f: # PATH_OF_DATAFILE is a variable stored in a .env file 
        json.dump(myjson, f)

def local_to_json_stage(): 
    """ This Method will fetch data from docker container and push it to snowflake stage
    Args: 
        - No arguments
    Returns:
        - This method returns nothing
    """
    try:
        execute_snowflake_commands("create or replace file format my_json_format type = 'json' strip_outer_array = true;")
        execute_snowflake_commands("create stage if not exists json_temporary_stage file_format = my_json_format;")
        execute_snowflake_commands("remove @json_temporary_stage/json_data.json.gz")
        execute_snowflake_commands("put file:///opt/airflow/dags/json_data.json @json_temporary_stage;")
    except Exception as e:
        print('SQL command of local_to_json_stage function failed, Exception: ', e)
    
def stage_to_table(): 
    """ This Method will copy that data form json stage and load it into snowflake table with varient data type column
    Args: 
        - No arguments
    Returns:
        - This method returns nothing
    """
    try:
        execute_snowflake_commands("create table if not exists json_data( json_data_raw variant );")
        execute_snowflake_commands("copy into json_data from  @json_temporary_stage/json_data.json;")
        #execute_snowflake_commands("copy into json_data from  @json_temporary_stage/json_data.json on_error = 'skip_file';")

    except Exception as e:
        print('SQL command of stage_to_table function failed, Exception: ', e)

def load_data_to_final():
    """ This Method will take data from json table and flatten it and then store it to a final snowflake table using a stored procedure
     Args: 
         - No arguments
     Returns:
         - This method returns nothing
     """
    try:
        execute_snowflake_commands("CALL load_data_to_final_approach2();")
    except Exception as e:
        print('SQL command of flattendata_into_final_table function failed, Exception: ', e)