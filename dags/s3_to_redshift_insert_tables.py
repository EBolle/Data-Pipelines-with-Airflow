# This DAG inserts the data for the fact and dimension tables and performs several data quality checks

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.sql import SQLCheckOperator
from operators.load_dimension import LoadDimensionOperator
from operators.load_fact import LoadFactOperator
from operators.stage_redshift import StageToRedshiftOperator

from sql import create_tables, insert_tables, quality_checks

# DAG setup

default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'start_date': datetime.datetime.now(),
                'retries': 3,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False,
                'catchup': False}

dag = DAG('s3_to_redshift_insert_tables',
          description='Inserts data into the fact and dimension tables in Redshift',
          schedule_interval='@hourly',
          default_args=default_args)

# Operators

start_operator = DummyOperator(
    task_id='start_insert_tables',
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

dq_songplays = SQLCheckOperator(task_id='dq_songplays',
                                dag=dag,
                                conn_id='redshift',
                                sql=quality_checks.songplays)

dq_users = SQLCheckOperator(task_id='dq_users',
                            dag=dag,
                            conn_id='redshift',
                            sql=quality_checks.users)

dq_songs = SQLCheckOperator(task_id='dq_songs',
                            dag=dag,
                            conn_id='redshift',
                            sql=quality_checks.songs)

dq_artists = SQLCheckOperator(task_id='dq_artists',
                              dag=dag,
                              conn_id='redshift',
                              sql=quality_checks.artists)

dq_time = SQLCheckOperator(task_id='dq_time',
                           dag=dag,
                           conn_id='redshift',
                           sql=quality_checks.time)

end_operator = DummyOperator(
    task_id='end_insert_tables',
    dag=dag)

# Order of execution

start_operator >> [stage_events, stage_songs]

[stage_events, stage_songs] >> load_songplays
load_songplays >> dq_songplays
dq_songplays >> [load_users, load_songs, load_artists, load_time]

load_users >> dq_users
load_songs >> dq_songs
load_artists >> dq_artists
load_time >> dq_time

[dq_users, dq_songs, dq_artists, dq_time] >> end_operator
