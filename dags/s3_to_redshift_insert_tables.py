# This DAG inserts the data for the fact and dimension tables

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator

from sql import create_tables, insert_tables

# DAG setup

default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False}

dag = DAG('s3_to_redshift_insert_tables',
          description='Inserts data into the fact and dimension tables in Redshift',
          start_date=datetime.datetime.now(),
          schedule_interval='@once',
          default_args=default_args,
          catchup=False)

# Operators

start_operator = DummyOperator(
    task_id='begin_insert_tables',
    dag=dag)

stage_events = StageToRedshiftOperator(
    task_id='staging_events',
    dag=dag,
    create_table_sql=create_tables.staging_events,
    s3_bucket='udacity-dend',
    s3_key='log_data',
    schema='public',
    table='staging_events',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])

stage_songs = StageToRedshiftOperator(
    task_id='staging_songs',
    dag=dag,
    create_table_sql=create_tables.staging_songs,
    s3_bucket='udacity-dend',
    s3_key='song_data',
    schema='public',
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])

load_songplays = LoadFactOperator(
    task_id='load_fact_songplays',
    dag=dag,
    insert_table_sql=insert_tables.songplays,
    redshift_conn_id='redshift')

load_users = LoadDimensionOperator(
    task_id='load_dim_users',
    dag=dag,
    insert_table_sql=insert_tables.users,
    schema='public',
    table='users',
    truncate=False,
    redshift_conn_id='redshift')

load_songs = LoadDimensionOperator(
    task_id='load_dim_songs',
    dag=dag,
    insert_table_sql=insert_tables.songs,
    schema='public',
    table='songs',
    truncate=False,
    redshift_conn_id='redshift')

load_artists = LoadDimensionOperator(
    task_id='load_dim_artists',
    dag=dag,
    insert_table_sql=insert_tables.artists,
    schema='public',
    table='artists',
    truncate=False,
    redshift_conn_id='redshift')

load_time = LoadDimensionOperator(
    task_id='load_dim_time',
    dag=dag,
    insert_table_sql=insert_tables.time,
    schema='public',
    table='time',
    truncate=False,
    redshift_conn_id='redshift')

end_operator = DummyOperator(
    task_id='end_insert_tables',
    dag=dag)

# Order of execution

start_operator >> [stage_events, stage_songs]

[stage_events, stage_songs] >> load_songplays

load_songplays >> [load_users, load_songs, load_artists, load_time]

[load_users, load_songs, load_artists, load_time] >> end_operator