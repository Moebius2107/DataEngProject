import airflow
import datetime
import urllib.request as request
import pandas as pd
import requests
import random
import json
import glob
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

ingestion_data_dag = DAG(
    dag_id='ingestion_data_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/usr/local/airflow/data/']
)


def _get_urls(dropbox_token, dropbox_link, destination_folder):
    import dropbox
    dbx = dropbox.Dropbox("sl.BTRxFPLrf6cuzF-WgIaNgg0aB-VPj10jvMTMXEg3j1G4NI4NN3QHBVIBlBY32nzNeLxJ09pImlST64c2Eo_EMFWgsiM2hPTLMUr7D0OOl7EGt5Yr2bcZ1SMuGF67QYAIZDGPltiHTEFV")
    link = dropbox.files.SharedLink(url=dropbox_link)

    entries = dbx.files_list_folder(path="", shared_link=link).entries

    for entry in entries:
        file_path = '/'+entry.name
        download_destination_path = destination_folder + file_path
        res = dbx.sharing_get_shared_link_file_to_file(download_destination_path, dropbox_link, path=file_path).url

download_hackatons_node = PythonOperator(
    task_id='download_hackaton_url_content',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=_get_urls,
    op_kwargs={
        "dropbox_token": "",
        "dropbox_link": "https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACsy_Ll8IgUjXujQSVR4KUIa/hackathons?dl=0&lst=",
        "destination_folder": "./raw_hackaton_data",
    },
    depends_on_past=False,
)

download_participants_node = PythonOperator(
    task_id='download_participant_url_content',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=_get_urls,
    op_kwargs={
        "dropbox_token": "",
        "dropbox_link": "https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AACnkkHEropuFClu7XgbhPuja/participants?dl=0&lst=",
        "destination_folder": "./raw_participant_data",
    },
    depends_on_past=False,
)

download_projects_node = PythonOperator(
    task_id='download_project_url_content',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=_get_urls,
    op_kwargs={
        "dropbox_token": "",
        "dropbox_link": "https://www.dropbox.com/sh/4i4tp6y0kl2lk24/AABMXKB4WetwcT_f1YoNtpbDa/projects?dl=0&lst=",
        "destination_folder": "./raw_project_data",
    },
    depends_on_past=False,
)



eleventh_node = DummyOperator(
    task_id='finale',
    dag=ingestion_data_dag,
    trigger_rule='none_failed'
)

[download_hackatons_node, download_participants_node, download_projects_node] >> eleventh_node