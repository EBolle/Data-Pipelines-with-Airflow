# This DAG creates the fact and dimension tables and should - normally - only be run once

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from sql import create_tables

# DAG setup

default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False}

dag = DAG('s3_to_redshift_create_tables',
          description='Creates the fact and dimension tables in Redshift',
          start_date=datetime.datetime.now(),
          schedule_interval='@once',
          default_args=default_args,
          catchup=False)

# Operators

start_operator = DummyOperator(
    task_id='begin_create_tables',
    dag=dag)

create_songplays_table = PostgresOperator(
    task_id='create_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.songplays)

create_songs_table = PostgresOperator(
    task_id='create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.songs)

create_users_table = PostgresOperator(
    task_id='create_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.users)

create_artists_table = PostgresOperator(
    task_id='create_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.artists)

create_time_table = PostgresOperator(
    task_id='create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.time)

end_operator = DummyOperator(
    task_id='end_create_tables',
    dag=dag)

# Order of execution

start_operator >> [create_songplays_table, create_songs_table, create_users_table,
                   create_artists_table, create_time_table]

[create_songplays_table, create_songs_table, create_users_table, create_artists_table, create_time_table] >> end_operator
