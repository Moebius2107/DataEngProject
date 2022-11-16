import airflow
import datetime
import urllib.request as request
import pandas as pd
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
import requests
import random
import json
import glob
import pandas as pd

default_args_dict = {
    'start_date': datetime.datetime(2020, 6, 25, 0, 0, 0),
    'concurrency': 1,
    'schedule_interval': "0 0 * * *",
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

ingestion_data_dag = DAG(
    dag_id='ingestion_data_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/usr/local/airflow/data/']
)


# Downloading a file from an API/endpoint?

#def _download_data():

def _download_url_content(url):
    try:
        url = url.replace('dl=0', 'dl=1')
        r= requests.get(url)
        print(r.content)
        return r.content
    except Exception as e:
        print('Exception in download_url():', e)

def get_data(urls_to_download, destination_file):
    with open(destination_file, 'wb') as destination:
        for url in urls_to_download:
            data = _download_url_content(url)
            destination.write(data)


first_node = PythonOperator(
    task_id='get_hackatons',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=get_data,
    op_kwargs={
        "urls_to_download": "/usr/local/airflow/data",
        "destination_file": "races",
    },
    depends_on_past=False,
)

second_node = PythonOperator(
    task_id='get_participants',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=get_data,
    op_kwargs={
        "urls_to_download": "/usr/local/airflow/data",
        "destination_file": "races",
    },
    depends_on_past=False,
)

third_node = PythonOperator(
    task_id='get_projects',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=get_data,
    op_kwargs={
        "urls_to_download": "/usr/local/airflow/data",
        "destination_file": "races",
    },
    depends_on_past=False,
)











