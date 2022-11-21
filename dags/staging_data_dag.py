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


default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

staging_data_dag = DAG(
    dag_id='staging_data_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

def get_mongodb_data(collection_name):
    warnings.filterwarnings('ignore')
    myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
    mydb = myclient["customer_db"]
    collection = mydb.collection_name
    data = pd.DataFrame(list(collection.find()))


get_hackaton_data = PythonOperator(
    task_id='get_hackaton_data',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=get_mongodb_data,
    op_kwargs={
        "collection_name": "hackatons",
    },
    depends_on_past=False,
)

get_participant_data = PythonOperator(
    task_id='get_participant_data',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=get_mongodb_data,
    op_kwargs={
        "collection_name": "participants",
    },
    depends_on_past=False,
)

get_project_data = PythonOperator(
    task_id='get_project_data',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=get_mongodb_data,
    op_kwargs={
        "collection_name": "projects",
    },
    depends_on_past=False,
)

eleventh_node = DummyOperator(
    task_id='finale',
    dag=staging_data_dag,
    trigger_rule='none_failed'
)

