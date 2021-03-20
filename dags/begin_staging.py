import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'retries': 3,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False}


dag = DAG('start_to_create_empty_tables',
          description='Create tables in RedShift',
          start_date=datetime.datetime(2021, 3, 1),
          schedule_interval='@once',
          default_args=default_args,
          catchup=False)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = PostgresOperator(
    task_id='create_tables',
    dag=dag,
    postgres_conn_id='redshift',
    sql="sql/create_tables.sql")

start_operator >> stage_events_to_redshift