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

staging_data_dag = DAG(
    dag_id='staging_data_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)

def _connect_to_db(host, port, db_name, username=None, password=None):
    if username and password:
         mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (username, password, host, port, db_name)
         conn = MongoClient(mongo_uri)
    else:
         conn = MongoClient(host, port)

    if not(conn.list_database_names().__contains__(db_name)):
        raise AirflowException("Error: database doesn't exist")
    return conn[db_name]

def _check_configuration(host, port, db_name, collection_names, username=None, password=None):
    mydb=_connect_to_db(host, port, db_name, username, password)
    for collection in collection_names:
        if not(mydb.list_collection_names().__contains__(collection)):
            raise AirflowException("Error: %s doesn't exist", collection)
                
    return True    

check_db_existence_node=PythonOperator(
    task_id='check_db_existence',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=_check_configuration,
    op_kwargs={
        "path": "/opt/airflow/dags/data/raw_hackaton_data",
        "db_name": "data_eng_db",
        "host":"mongo",
        "port":27017, 
        "collection_names": ['hackatons', 'projects', 'participants']
    },
    depends_on_past=False,
)

def _transform_to_frame(collection_name, collection_frame_name, host, port, db_name, username=None, password=None):
    mydb=_connect_to_db(host, port, db_name, username=None, password=None)
    collection_frame_name  = pd.DataFrame(list(mydb[collection_name].find()))
    collection_no_duplicated = collection_frame_name
    if collection_frame_name.duplicated().contains(True):
        collection_no_duplicated = collection_frame_name.drop_duplicates()

def _cleaning_participants(collection_frame_name, host, port, db_name, username=None, password=None):
    mydb=_connect_to_db(host, port, db_name, username=None, password=None)
    participants_frame  = pd.DataFrame(list(mydb['participants'].find()))
    participants_no_duplicata = participants_frame.groupby('participant-url').first().reset_index()
    participants_no_columns = participants_no_duplicata.drop(columns=['participant-linkedin', 'participant-twitter', 'participant-bio', 'participant-followers', 'participant-likes'])
    
    

# get_hackaton_dataframe_node = PythonOperator(
#     task_id='get_hackaton_dataframe',
#     dag=staging_data_dag,
#     trigger_rule='none_failed',
#     python_callable=_transform_to_frame,
#     op_kwargs={
#         "db_name": "data_eng_db",
#         "host":"mongo",
#         "port":27017, 
#         "collection_name": 'hackatons',
#     },
#     depends_on_past=False,
# )

# get_participant_dataframe_node = PythonOperator(
#     task_id='get_participant_dataframe',
#     dag=staging_data_dag,
#     trigger_rule='none_failed',
#     python_callable=_transform_to_frame,
#     op_kwargs={
#         "db_name": "data_eng_db",
#         "host":"mongo",
#         "port":27017, 
#         "collection_name": 'participants'
#     },
#     depends_on_past=False,
# )


# get_project_dataframe_node = PythonOperator(
#     task_id='get_project_dataframe',
#     dag=staging_data_dag,
#     trigger_rule='none_failed',
#     python_callable=_transform_to_frame,
#     op_kwargs={
#         "db_name": "data_eng_db",
#         "host":"mongo",
#         "port":27017, 
#         "collection_name": "projects",
#     },
#     depends_on_past=False,
# )

wrangling_hackatons_data_node = DummyOperator(
    task_id='wrangling_hackatons_data',
    dag=staging_data_dag,
    trigger_rule='none_failed'
)
wrangling_participants_data_node = DummyOperator(
    task_id='wrangling_participants_data',
    dag=staging_data_dag,
    trigger_rule='none_failed'
)
wrangling_projects_data_node = DummyOperator(
    task_id='wrangling_projects_data',
    dag=staging_data_dag,
    trigger_rule='none_failed'
)

merge_dataframe_node = DummyOperator(
    task_id='merge_dataframe',
    dag=staging_data_dag,
    trigger_rule='none_failed'
)

# def _delete_duplicated_data(frame_name) : 
    

# delete_duplicate_hackaton_node = PythonOperator(
#     task_id='delete_duplicate_hackaton',
#     dag=staging_data_dag,
#     trigger_rule='none_failed',
#     python_callable=_delete_duplicated_data,
#     op_kwargs={
#         "frame_name": "hackaton"
#     },
#     depends_on_past=False,
# )

finale_node = DummyOperator(
    task_id='finale',
    dag=staging_data_dag,
    trigger_rule='none_failed'
)

check_db_existence_node >> [wrangling_projects_data_node, wrangling_hackatons_data_node, wrangling_participants_data_node] >> merge_dataframe_node >> finale_node
#check_db_existence_node >> [get_hackaton_dataframe_node, get_project_dataframe_node, get_participant_dataframe_node] >> finale_node