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


default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

production_data_dag = DAG(
    dag_id='production_data_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

