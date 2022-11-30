import airflow
import datetime
import urllib.request as request
import pandas as pd
import requests
import pymongo
import json
import glob
from pprint import pprint
import warnings
from pymongo import MongoClient
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow import AirflowException

args={'owner': 'airflow'}

default_args_dict = {
    'owner': 'airflow', 
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

production_data_dag = DAG(
    dag_id='production_data_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

create_table = PostgresOperator(
    task_id="create_table",
    dag = production_data_dag,
    postgres_conn_id='postgres_not_default',
    sql="sql/table_schema.sql",
    trigger_rule='all_success',
    autocommit=True
)

populate_table = PostgresOperator(
    task_id="populate_table",
    dag = production_data_dag,
    postgres_conn_id="postgres_not_default",
    sql="sql/table_data.sql",
)

finale_node = DummyOperator(
    task_id='finale',
    dag=production_data_dag,
    trigger_rule='none_failed'
)

create_table >> populate_table >> finale_node


