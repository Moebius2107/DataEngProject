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
    collection_frame_name.to_csv('/opt/airflow/dags/staging/' +collection_name +'.csv')

    
#In the next 3 steps we create the different 3 dataframes we are going to use from the MongoDB Database.

get_hackaton_dataframe_node = PythonOperator(
    task_id='get_hackaton_dataframe',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=_transform_to_frame,
    op_kwargs={
        "db_name": "data_eng_db",
        "host":"mongo",
        "port":27017, 
        "collection_name": 'hackatons',
    },
    depends_on_past=False,
)


get_participant_dataframe_node = PythonOperator(
    task_id='get_participant_dataframe',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=_transform_to_frame,
    op_kwargs={
        "db_name": "data_eng_db",
        "host":"mongo",
        "port":27017, 
        "collection_name": 'participants'
    },
    depends_on_past=False,
)


get_project_dataframe_node = PythonOperator(
    task_id='get_project_dataframe',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=_transform_to_frame,
    op_kwargs={
        "db_name": "data_eng_db",
        "host":"mongo",
        "port":27017, 
        "collection_name": "projects",
    },
    depends_on_past=False,
)

#in the next 3 taks we are going to reproduce the same steps that we have in our notebook attached in the solution to wrangle the 3 dataframes

def wrangling_hackatons():
    hackatons = pd.read_csv('/opt/airflow/dags/staging/hackatons.csv')
    unique_hack = hackatons.drop_duplicates(subset="hackathon-id", keep='first')
    final_hackaton=unique_hack.dropna(axis=1).copy()
    final_hackaton['hackathon-location-address'] = unique_hack['hackathon-location-address']
    final_hackaton['hackathon-location-description'] = unique_hack['hackathon-location-description']
    final_hackaton['hackathon-location-google-maps'] = unique_hack['hackathon-location-google-maps']
    final_hackaton = final_hackaton.drop('hackathon-number-of-judging-criteria', axis=1)
    final_hackaton = final_hackaton.drop('hackathon-location-description', axis=1)
    final_hackaton = final_hackaton.dropna(subset=['hackathon-location-address', 'hackathon-location-google-maps'], how = 'all')
    final_hackaton = final_hackaton.dropna(subset=['hackathon-id'], how = 'all')
    final_hackaton['hackathon-location-address'].fillna(final_hackaton['hackathon-location-google-maps'])
    located_hack[['City', 'Code', 'Country']] = final_hackaton['hackathon-location-address'].str.rsplit(',', 2,  expand=True)
    final_hackaton['Country'] = located_hack['Country']
    final_hackaton.sample(20)
    final_hackaton = final_hackaton.dropna(subset=['Country'], how = 'all')
    final_hackaton = final_hackaton.drop('hackathon-location-address', axis=1)
    final_hackaton = final_hackaton.drop('hackathon-location-google-maps', axis=1)
    final_hackaton.to_csv('/opt/airflow/dags/staging/hackatons_for_merge.csv')


wrangling_hackatons_data_node = PythonOperator(
    task_id='wrangling_hackatons_data_node',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=wrangling_hackatons,
    depends_on_past=False,
)

def wrangling_participants():
    participants = pd.read_csv('/opt/airflow/dags/staging/participants.csv')
    participants = participants.drop('participant-twitter', axis =1)
    participants = participants.drop('_id', axis=1)
    participants = participants[participants.columns.drop(list(participants.filter(regex='participant-skills.')))]
    unique_part = participants.drop_duplicates(subset="participant-id", keep='first')
    unique_part.to_csv('/opt/airflow/dags/staging/participants_for_merge.csv')

wrangling_participants_data = PythonOperator(
    task_id='wrangling_participants_data',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=wrangling_participants,
    depends_on_past=False,
)

def wrangling_projects():
    projects = pd.read_csv('/opt/airflow/dags/staging/projects.csv')
    unique_projects = projects.drop_duplicates(subset="project-id", keep='first')
    unique_projects = unique_projects.dropna(subset=['hackathon-id'], how = 'all')
    final_projects = unique_projects[unique_projects.columns.drop(list(unique_projects.filter(regex='project-technologies')))]
    final_projects = final_projects[final_projects.columns.drop(list(final_projects.filter(regex='participant-bubble')))]
    final_projects = final_projects[final_projects.columns.drop(list(final_projects.filter(regex='participant-desc')))]
    final_projects = final_projects.drop(['project-video', 'project-basis', 'project-purpose', 'project-inspiration', 'project-lessons-learned'], axis=1)
    final_p = final_projects.melt(id_vars = ['hackathon-id', 'hackathon-winner', 'project-challenges', 'project-future-plans', 'hackathon-url', 'project-title', 'project-accomplishments', 'project-likes', 'hackathon-name', 'project-subtitle', 'project-github-url', 'project-number-of-comments', 'team-size', 'project-creation-timestamp', 'project-url', 'project-id'], value_vars = ['team.0.participant-id','team.1.participant-id', 'team.2.participant-id','team.3.participant-id','team.4.participant-id','team.5.participant-id']  ,var_name = 'team', value_name = 'participant-id')
    non_null_p = final_p.dropna(subset=['participant-id'], how = 'all')
    non_null_p.to_csv('/opt/airflow/dags/staging/projects_for_merge.csv')

wrangling_projects_data_node = PythonOperator(
    task_id='wrangling_projects_data_node',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=wrangling_projects,
    depends_on_past=False,
)

#Now after having our 3 dataframes cleaned and ready to be analyzed we are going to merge them to end the staging part

def merge_datasets():
    participants = pd.read_csv('/opt/airflow/dags/staging/participants_for_merge.csv')
    projects = pd.read_csv('/opt/airflow/dags/staging/projects_for_merge.csv')
    hackatons = pd.read_csv('/opt/airflow/dags/staging/hackatons_for_merge.csv')
    joined_data = projects.join(hackatons.set_index('hackathon-id'), how='inner', on='hackathon-id', lsuffix='_caller')
    joined_data_0= pd.merge(joined_data, participants, left_on='participant-id' , right_on='participant-id')
    joined_data_0 = joined_data_0.drop('project-challenges', axis=1)
    joined_data_0 = joined_data_0.drop('project-future-plans', axis=1)
    joined_data_0 = joined_data_0.drop('project-accomplishments', axis=1)
    joined_data_0 = joined_data_0.drop('hackathon-description-text', axis=1)
    joined_data_0.to_csv('/opt/airflow/dags/staging/final_prodution_data.csv')

merge_dataframe_node = PythonOperator(
    task_id='wrangling_hackatons_data_node',
    dag=staging_data_dag,
    trigger_rule='none_failed',
    python_callable=merge_datasets,
    depends_on_past=False,
)

#we delete files used in staging in exception of the one to persist staging data

clean_projects_file_node= BashOperator(
    task_id = 'clean_projects_file_node',
    dag = staging_data_dag,
    trigger_rule='none_failed',
    bash_command='rm -rf /opt/airflow/dags/staging/projects_for_merge.csv',
)

clean_hackatons_file_node= BashOperator(
    task_id = 'clean_hackatons_file_node',
    dag = staging_data_dag,
    trigger_rule='none_failed',
    bash_command='rm -rf /opt/airflow/dags/staging/hackatons_for_merge.csv',
)

clean_participants_file_node= BashOperator(
    task_id = 'clean_participants_file_node',
    dag = staging_data_dag,
    trigger_rule='none_failed',
    bash_command='rm -rf /opt/airflow/dags/staging/participants_for_merge.csv',
)




finale_node = DummyOperator(
    task_id='finale',
    dag=staging_data_dag,
    trigger_rule='none_failed'
)

check_db_existence_node >> get_hackaton_dataframe_node >> get_participant_dataframe_node >> get_project_dataframe_node >> wrangling_hackatons_data_node >>  wrangling_participants_data  >> wrangling_projects_data_node >> merge_dataframe_node >> clean_projects_file_node >> clean_hackatons_file_node >>  clean_participants_file_node >> finale_node
