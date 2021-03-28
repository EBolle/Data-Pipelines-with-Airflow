# This DAG checks the data quality of the fact and dimension tables

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import SQLCheckOperator
from airflow.sensors.external_task import ExternalTaskSensor

from sql import quality_checks

# DAG setup

default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False}

dag = DAG('s3_to_redshift_data_quality_checks',
          description='Checks the data quality of the fact and dimension tables in Redshift',
          start_date=datetime.datetime.now(),
          schedule_interval='@once',
          default_args=default_args,
          catchup=False)

# Operators

wait_for_insert_tables = ExternalTaskSensor(
    task_id='dq_sensor',
    external_dag_id='s3_to_redshift_insert_tables',
    external_task_id='end_insert_tables')

start_operator = DummyOperator(
    task_id='begin_data_quality_checks',
    dag=dag)

dq_songplays = SQLCheckOperator(task_id='dq_songplays',
                                dag=dag,
                                sql=quality_checks.songplays)

dq_users = SQLCheckOperator(task_id='dq_users',
                            dag=dag,
                            sql=quality_checks.users)

dq_songs = SQLCheckOperator(task_id='dq_songs',
                            dag=dag,
                            sql=quality_checks.songs)

dq_artists = SQLCheckOperator(task_id='dq_artists',
                              dag=dag,
                              sql=quality_checks.artists)

dq_time = SQLCheckOperator(task_id='dq_time',
                           dag=dag,
                           sql=quality_checks.time)

end_operator = DummyOperator(
    task_id='end_data_quality_checks',
    dag=dag)

# Order of execution

wait_for_insert_tables >> start_operator
start_operator >> [dq_songplays, dq_users, dq_songs, dq_artists, dq_time]
[dq_songplays, dq_users, dq_songs, dq_artists, dq_time] >> end_operator
