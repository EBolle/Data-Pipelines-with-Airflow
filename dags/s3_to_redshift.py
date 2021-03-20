  # Airflow DAG which extracts, loads, and transform data from S3 to Redshift

import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_redshift import S3ToRedshiftOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


from sql.create_tables import

