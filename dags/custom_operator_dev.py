# Development DAG to write and test new customized Operators

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from operators.stage_redshift import StageToRedshiftOperator

from sql import create_tables


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


stage_events = StageToRedshiftOperator(
    task_id='staging_events',
    create_table_sql=create_tables.staging_events,
    s3_bucket='udacity-dend',
    s3_key='log_data',
    schema='PUBLIC',
    table='staging_events',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])


start_operator >> stage_events



