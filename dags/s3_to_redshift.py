  # Airflow DAG which extracts, loads, and transform data from S3 to Redshift

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from sql import create_tables, insert_tables


  # DAG setup

default_args = {'owner': 'airflow',
                'depends_on_past': False,
                'retries': 3,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False}

dag = DAG('s3_to_redshift',
          description='ETL .json files on S3 to Redshift',
          start_date=datetime.datetime.now(),
          schedule_interval='@once',
          default_args=default_args,
          catchup=False)

  # DAG Operators

start_operator = PostgresOperator(
    task_id='Begin_execution',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables)

load_staging_events = S3ToRedshiftOperator(
    task_id='Stage_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    schema='PUBLIC',
    table='staging_events',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])

load_staging_song = S3ToRedshiftOperator(
    task_id='Stage_song',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    schema='PUBLIC',
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])

load_songplays = PostgresOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.songplays)

load_users = PostgresOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.users)

load_songs = PostgresOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.songs)

load_artist = PostgresOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.artist)

load_time = PostgresOperator(
    task_id='load_time_fact_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.time)

  # Task order

start_operator >> load_staging_events
start_operator >> load_staging_song

load_staging_events >> load_songplays
load_staging_song >> load_songplays

load_songplays >> load_users
load_songplays >> load_songs
load_songplays >> load_artist
load_songplays >> load_time
