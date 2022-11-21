import airflow
import datetime
import urllib.request as request
import requests
import random
import json
import glob
from pymongo import MongoClient
from pprint import pprint
import warnings
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
    template_searchpath=['/opt/airflow/dags/']
)

clean_hackatons = BashOperator(
    task_id = 'clean_hackatons',
    dag = ingestion_data_dag,
    trigger_rule='none_failed',
    bash_command='rm /opt/airflow/dags/data/raw_hackaton_data/*',
)


clean_participants = BashOperator(
    task_id = 'clean_participants',
    dag = ingestion_data_dag,
    trigger_rule='none_failed',
    bash_command='rm /opt/airflow/dags/data/raw_participant_data/*',
)


clean_projects = BashOperator(
    task_id = 'clean_projects',
    dag = ingestion_data_dag,
    trigger_rule='none_failed',
    bash_command='rm /opt/airflow/dags/data/raw_project_data/*',
)

def _get_urls(dropbox_token, dropbox_link, destination_folder):
    import dropbox
    dbx = dropbox.Dropbox("sl.BThg5FS_elL0xsQVQU2BiH1GMCzVRRMvgd_KaspnXVEqUtVirvOwN2Vsa-kt5tcNWnZXh8OljlOZEWUvLh1WRjr4F1gF6de-OeBcAjPjF_hNzILd7BUAgKCumjQjxyuPHRgi9n1gZ6au")
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
        "destination_folder": "/opt/airflow/dags/data/raw_hackaton_data",
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
        "destination_folder": "/opt/airflow/dags/data/raw_participant_data",
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
        "destination_folder": "/opt/airflow/dags/data/raw_project_data",
    },
    depends_on_past=False,
)


# def create_collections():
#     warnings.filterwarnings('ignore')

#     #we use the MongoClient to communicate with the running database instance.
#     myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
#     mydb = myclient["customer_db"]

#     #Or you can use the attribute style 
#     #mydb = myclient.customer_db

#     print(mydb.list_collection_names())
#     participants = mydb["participants"]
#     hackatons = mydb["hackatons"]
#     projects = mydb["projects"]

def ingest_collection(destination_collection, path, db):
    import os
    import json
    # assign directory
    directory = path
    
    # iterate over files in
    # that directory
    warnings.filterwarnings('ignore')
    myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
    mydb = myclient["customer_db"]
    collection = mydb[destination_collection]
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if os.path.isfile(f):
            with open(directory+'/'+filename) as file:
                file_data = json.load(file)
                x = collection.insert_one(file_data)

# create_collection_node = PythonOperator(
#     task_id='create_collections',
#     dag=ingestion_data_dag,
#     trigger_rule='none_failed',
#     python_callable=create_collections,
#     op_kwargs={},
#     depends_on_past=False,
# )



ingest_mongo_hackaton_node = PythonOperator(
    task_id='ingest_hackaton',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=ingest_collection,
    op_kwargs={
        "destination_collection": "hackatons",
        "path": "/opt/airflow/dags/data/raw_hackaton_data",
        "db": "mydb",
    },
    depends_on_past=False,
)

ingest_mongo_participant_node = PythonOperator(
    task_id='ingest_participant',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=ingest_collection,
    op_kwargs={
        "destination_collection": "participants",
        "path": "/opt/airflow/dags/data/raw_participant_data",
        "db": "mydb",
    },
    depends_on_past=False,
)

ingest_mongo_project_node = PythonOperator(
    task_id='ingest_project',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=ingest_collection,
    op_kwargs={
        "destination_collection": "projects",
        "path": "/opt/airflow/dags/data/raw_project_data",
        "db": "mydb",
    },
    depends_on_past=False,
)


dummy_node = DummyOperator(
    task_id='dummy_node',
    dag=ingestion_data_dag)

closing_node = DummyOperator(
    task_id='finale',
    dag=ingestion_data_dag,
    trigger_rule='none_failed'
)


[clean_hackatons, clean_projects, clean_participants] >> dummy_node >> [download_hackatons_node, download_participants_node, download_projects_node] >> dummy_node >> [ingest_mongo_hackaton_node, ingest_mongo_participant_node, ingest_mongo_project_node] >> clean_hackatons >> clean_participants >> clean_projects >> closing_node
 