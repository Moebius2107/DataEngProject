import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

master_dag = DAG(
    dag_id='master_dag',
    default_args=default_args_dict,
    catchup=False,
    template_searchpath=['/opt/airflow/dags/']
)


trigger_ingestion_dag_node = TriggerDagRunOperator(
        task_id="trigger_ingestion_dag",
        dag=master_dag,
        trigger_dag_id="ingestion_data_dag",  # Ensure this equals the dag_id of the DAG to trigger
        reset_dag_run=True,
        wait_for_completion=True,
    )

trigger_staging_dag_node = TriggerDagRunOperator(
        task_id="trigger_staging_dag",
        dag=master_dag,
        trigger_dag_id="staging_data_dag",  # Ensure this equals the dag_id of the DAG to trigger
        reset_dag_run=True,
        wait_for_completion=True,
    )

closing_node = DummyOperator(
    task_id='finale_master',
    dag=master_dag,
    trigger_rule='none_failed'
)

trigger_ingestion_dag_node >> trigger_staging_dag_node >> closing_node