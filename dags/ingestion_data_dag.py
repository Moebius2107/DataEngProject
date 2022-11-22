import airflow
import datetime
import urllib.request as request
import requests
import json
import glob
import os
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

"""clean_folders_node is used to have the files clean before retrieving the data."""
clean_folders_node= BashOperator(
    task_id = 'clean_folders',
    dag = ingestion_data_dag,
    trigger_rule='none_failed',
    bash_command='rm -rf /opt/airflow/dags/data/*',
)

"""_get_urls function is used to retrieve all the files stored at the dropbox_link address and then save them in the destination_folder"""
def _get_urls(dropbox_token, dropbox_link, destination_folder):
    import dropbox
    import logging
    #Verify if the destination_folder exist, if not then create it
    if not os.path.isdir(destination_folder):
        os.makedirs(destination_folder)

      #Token of our DropBox application needed to download a file shared by another user
    dbx = dropbox.Dropbox(app_key = 'shzy2qdkfpbbxtc',app_secret ='wrdl22673rak9fr',oauth2_refresh_token ="cY3v-w-rOWIAAAAAAAAAAb7Lnd3nxlJM_WXVUobcu1VNz_V0Ue1HdjaSrpM7yCeb")
    #     dbx = dropbox.Dropbox("sl.BTi5JEe-iWXKSOnNPX5NGnWR4ZoCwUaIgvL4fvkBCM-fx14sJFg_dg9DsMgxBhfqkcIoe-8H7XmaU9HGhDLwlmwDBftMilRKeCvZUna7_nG4PP4dBHBd53qybYw5FLBNPtYsB6A776EI")
    link = dropbox.files.SharedLink(url=dropbox_link)

    entries = dbx.files_list_folder(path="", shared_link=link).entries

    for entry in entries:
        file_path = '/'+entry.name
        download_destination_path = destination_folder + file_path
        res = dbx.sharing_get_shared_link_file_to_file(download_destination_path, dropbox_link, path=file_path).url

"""download_hackatons_node is used to download all the files about hackatons"""
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

"""download_participants_node is used to download all the files about participants"""
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

"""download_projects_node is used to download all the files about the projects"""
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


# def _create_collections():
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



# def _connect_mongo(host, port,  db, username=None, password=None):

#     global _connection_to_db

#     if username and password:
#         mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db)
#         conn = MongoClient(mongo_uri)
#     else:
#         conn = MongoClient(host, port)

#     _connection_to_db = conn[db]

# connect_mongodb_node = PythonOperator(
#     task_id='connect_mongo',
#     dag=ingestion_data_dag,
#     trigger_rule='none_failed',
#     python_callable=_connect_mongo,
#     op_kwargs={
#         "host": "localhost",
#         "port": 27017,
#         "db": "data_eng_db",
#     },
#     depends_on_past=False,
# )

def _ingest_collection(destination_collection, path, host, port, db_name, username=None, password=None):
    import json
    # assign directory
    directory = path
    
    # iterate over files in
    # that directory
    warnings.filterwarnings('ignore')

    if username and password:
         mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db_name)
         conn = MongoClient(mongo_uri)
    else:
         conn = MongoClient(host, port)

    mydb = conn[db_name]

    #myclient = MongoClient("mongodb://mongo:27017/") #Mongo URI format
    #mydb = myclient["data_eng_db"]

    collection = mydb[destination_collection]
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        # checking if it is a file
        if f.endswith('.json'):
            with open(f) as file:
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
    python_callable=_ingest_collection,
    op_kwargs={
        "destination_collection": "hackatons",
        "path": "/opt/airflow/dags/data/raw_hackaton_data",
        "db_name": "data_eng_db",
        "host":"mongo",
        "port":27017, 
    },
    depends_on_past=False,
)

ingest_mongo_participant_node = PythonOperator(
    task_id='ingest_participant',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=_ingest_collection,
    op_kwargs={
        "destination_collection": "participants",
        "path": "/opt/airflow/dags/data/raw_participant_data",
        "db_name": "data_eng_db",
        "host":"mongo",
        "port":27017,
    },
    depends_on_past=False,
)

ingest_mongo_project_node = PythonOperator(
    task_id='ingest_project',
    dag=ingestion_data_dag,
    trigger_rule='none_failed',
    python_callable=_ingest_collection,
    op_kwargs={
        "destination_collection": "projects",
        "path": "/opt/airflow/dags/data/raw_project_data",
        "db_name": "data_eng_db",
        "host":"mongo",
        "port":27017,
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


clean_folders_node >> [download_hackatons_node, download_participants_node, download_projects_node] >> dummy_node >> [ingest_mongo_hackaton_node, ingest_mongo_participant_node, ingest_mongo_project_node] >>  closing_node
 