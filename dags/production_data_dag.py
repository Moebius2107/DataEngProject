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

# def _insert(output_folder):
#     df = pd.read_csv('/opt/airflow/dags/data_to_prod.csv')
#     idx = 0
#     df.insert(idx, 'index', value=range(len(df)))
#     df.set_index('index')
#     with open("/opt/airflow/dags/sql/inserts.sql", "w") as f:
#         df_iterable = df.iterrows()
#         f.write(
#             "CREATE TABLE IF NOT EXISTS hackaton_details (\n"
#             "hackathon_id text not null,\n"
#             "hackathon_winner boolean,\n"   
#             "hackathon_url_caller text,\n"    
#             "project_title text,\n"    
#             "project_likes int,\n"   
#             "hackathon_name text,\n"    
#             "project_subtitle text,\n"   
#             "project_github_url text,\n"  
#             "project_number_of_comments int,\n"    
#             "team_size int,\n"   
#             "project_creation_timestamp timestamp,\n"    
#             "project_url text,\n"   
#             "project_id text not null,\n"   
#             "team text,\n"   
#             "participant_id text not null,\n"   
#             "_id text,\n"   
#             "hackathon_number_of_prizes int,\n"    
#             "hackathon_number_of_judges int,\n"   
#             "hackathon_number_of_participants int,\n"   
#             "hackathon_is_colocated boolean,\n"   
#             "hackathon_has_ended boolean,\n"   
#             "hackathon_url text,\n"   
#             "hackathon_description_header text,\n"    
#             "hackathon_prizes_total text,\n"   
#             "has_projects boolean,\n"    
#             "country text,\n"   
#             "participant_likes int,\n"   
#             "participant_projects int,\n"   
#             "participant_url text,\n"  
#             "participant_name text,\n"   
#             "participant_followers text,\n"   
#             "participant_location text,\n" 
#             "participant_bio text,\n"
#             "participant_linkedin text,\n"  
#             "participant_website text,\n"   
#             "participant_github text,\n"   
#             "participant_hackathons text,\n"   
#             "participant_following text,\n"   
#             "participant_number_of_skills int\n"
#             ");\n\n"
#         )
#         for index, row in df_iterable:
#            index = row['index']
#            hackathon_id = row['hackaton-id']
#            hackathon_winner = row['hackathon-winner']
#            hackathon_url_caller = row['hackathon-url_caller']
#            project_title = row['project-title']
#            project_likes = row['project-likes']
#            hackathon_name = row['hackathon-name']
#            project_subtitle = row['project-subtitle']
#            project_github_url = row['project-github-url']
#            project_number_of_comments = row['project-number-of-comments']
#            team_size = row['team-size']
#            project_creation_timestamp = row['project-creation-timestamp']
#            project_url = row['project-url']
#            project_id = row['project-id']
#            team = row['team']
#            participant_id = row['participant-id']
#            _id = row['_id']
#            hackathon_number_of_prizes = row['hackathon-number-of-prizes']
#            hackathon_number_of_judges = row['hackathon-number-of-judges']
#            hackathon_number_of_participants = row['hackathon-number-of-participants']
#            hackathon_is_colocated = row['hackathon-is-colocated']
#            hackathon_has_ended = row['hackathon-has-ended']
#            hackathon_url = row['hackathon-url']
#            hackathon_description_header = row['hackathon-description-header']
#            hackathon_prizes_total = row['hackathon-prizes-total']
#            has_projects = row['has-projects']
#            country = row['Country']
#            participant_likes = row['participant-likes']
#            participant_projects = row['participant-projects']
#            participant_url = row['participant-url']
#            participant_name = row['participant-name']
#            participant_followers = row['participant-followers']
#            participant_location = row['participant-location']
#            participant_bio = row['participant-bio']
#            participant_linkedin = row['participant-linkedin']
#            participant_website = row['participant-website']
#            participant_github = row['participant-github']
#            participant_hackathons = row['participant-hackathons']
#            participant_following = row['participant-following']
#            participant_number_of_skills = row['participant-number-of-skills']
           
#            f.write(
#                 "INSERT INTO hackaton_details VALUES ("
#                 f"'{hackathon_id}', '{hackathon_winner}','{hackathon_url_caller}','{project_title}','{project_likes}','{hackathon_name}',"
#                 f"'{project_subtitle}','{project_github_url}','{project_number_of_comments}','{team_size}','{project_creation_timestamp}',"
#                 f"'{project_url}','{project_id}','{team}','{participant_id}','{_id}','{hackathon_number_of_prizes}',"
#                 f"'{hackathon_number_of_judges}','{hackathon_number_of_participants}','{hackathon_is_colocated}','{hackathon_has_ended}',"
#                 f"'{hackathon_url}','{hackathon_description_header}','{hackathon_prizes_total}','{has_projects}','{country}',"
#                 f"'{participant_likes}','{participant_projects}','{participant_url}','{participant_name}','{participant_followers}','{participant_location}',"
#                 f"'{participant_bio}','{participant_linkedin}','{participant_website}','{participant_github}','{participant_hackathons}','{participant_following}','{participant_number_of_skills}'"
#                 ");\n"
#             )

#         f.close()


# ninth_node = PythonOperator(
#     task_id='generate_insert',
#     dag=production_data_dag,
#     python_callable=_insert,
#     op_kwargs={
#         "output_folder": "/usr/local/airflow/data"
#     },
#     trigger_rule='none_failed',
# )

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


