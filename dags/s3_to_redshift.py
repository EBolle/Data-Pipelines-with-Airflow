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
                'retries': 0,
                'retry_delay': timedelta(minutes=5),
                'email_on_retry': False}


dag = DAG('s3_to_redshift',
          description='ETL .json files from S3 to Redshift',
          start_date=datetime.datetime.now(),
          schedule_interval='@once',
          default_args=default_args,
          catchup=False)

# DAG Operators

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)


create_staging_events = PostgresOperator(
    task_id='Create_staging_events',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.staging_events
)


create_staging_songs = PostgresOperator(
    task_id='Create_staging_songs',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.staging_songs
)


load_staging_events = S3ToRedshiftOperator(
    task_id='Load_staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    schema='PUBLIC',
    table='staging_events',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])


load_staging_songs = S3ToRedshiftOperator(
    task_id='Load_staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data',
    schema='PUBLIC',
    table='staging_songs',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])


create_songplays_table = PostgresOperator(
    task_id='Create_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.songplays
)


load_songplays_table = PostgresOperator(
    task_id='Load_songplays_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.songplays)


create_songs_table = PostgresOperator(
    task_id='Create_songs_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.songs
)


create_users_table = PostgresOperator(
    task_id='Create_users_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.users
)


create_artists_table = PostgresOperator(
    task_id='Create_artists_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.artists
)


create_time_table = PostgresOperator(
    task_id='Create_time_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=create_tables.time
)


load_songs_table = PostgresOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.songs)


load_users_table = PostgresOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.users)


load_artists_table = PostgresOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.artists)


load_time_table = PostgresOperator(
    task_id='Load_time_fact_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=insert_tables.time)


data_quality = DummyOperator(
    task_id='Run_data_quality_checks',
    dag=dag)


end = DummyOperator(
    task_id='End_execution',
    dag=dag)


# Order of execution


start_operator >> [create_staging_events, create_staging_songs]

create_staging_events >> load_staging_events
create_staging_songs >> load_staging_songs
[load_staging_events, load_staging_songs] >> create_songplays_table
create_songplays_table >> load_songplays_table

load_songplays_table >> [create_songs_table, create_users_table, create_artists_table, create_time_table]

create_songs_table >> load_songs_table
create_users_table >> load_users_table
create_artists_table >> load_artists_table
create_time_table >> load_time_table

[load_songs_table, load_users_table, load_artists_table, load_time_table] >> data_quality

data_quality >> end
