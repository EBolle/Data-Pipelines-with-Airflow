# Development DAG to write and test new customized Operators

import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import StageToRedshiftOperator

from sql import create_tables


# Operator 1: Staging the data
# Requirements: the task uses params to generate the copy statement dynamically
#             : The operator contains logging in different steps of the execution
#             : The SQL statements are executed by using a Airflow hook
# Idea: base a large part on the S3TORedshiftOperator and add your personal code to it
# Creates the staging table - loads the staging table - checks the staging table --> staging_dag !
# Note: check S3 bucket how to select based on date


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
    create_table_sql=create_tables.staging_events,
    quality_sql="SELECT count(*) FROM public.staging_events",
    task_id='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data',
    schema='PUBLIC',
    table='staging_events',
    redshift_conn_id='redshift',
    aws_conn_id='aws_credentials',
    copy_options=["JSON 'auto ignorecase'"])

start_operator >> stage_events



