  # Airflow DAG which extracts, loads, and transform data from S3 to Redshift

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from sql import create_tables


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

start_operator = PostgresOperator(task_id='start_execution',
                                  dag=dag,
                                  postgres_conn_id='redshift',
                                  sql=create_tables)

create_staging_events = PostgresOperator(
        task_id='create_staging_events',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.staging_events)

load_staging_events = S3ToRedshiftOperator(
        task_id='transfer_s3_to_staging_events',
        s3_bucket='udacity-dend',
        s3_key='log_data',
        schema='PUBLIC',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        copy_options=["JSON 'auto ignorecase'"])

create_staging_songs = PostgresOperator(
        task_id='create_staging_songs',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_tables.staging_songs)

load_staging_songs = S3ToRedshiftOperator(
        task_id='transfer_s3_to_staging_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        schema='PUBLIC',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        copy_options=["JSON 'auto ignorecase'"])

  # Task order

start_operator >> create_staging_events
start_operator >> create_staging_songs
create_staging_events >> load_staging_events
create_staging_songs >> load_staging_songs
