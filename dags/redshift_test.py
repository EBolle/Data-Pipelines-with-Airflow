import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


from sql_queries import create_staging_songs

dag = DAG('test_redshift_connection',
        start_date=datetime.datetime.now()
        )

start_operator = DummyOperator(task_id='begin_execution', dag=dag)

create_staging_songs = PostgresOperator(
        task_id='create_staging_songs',
        dag=dag,
        postgres_conn_id='redshift',
        sql=create_staging_songs)

load_staging_songs = S3ToRedshiftOperator(
        task_id='transfer_s3_to_stagings_songs',
        s3_bucket='udacity-dend',
        s3_key='song_data',
        schema='PUBLIC',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_conn_id='aws_credentials',
        copy_options=["JSON 'auto ignorecase'"])

start_operator >> create_staging_songs
create_staging_songs >> load_staging_songs


