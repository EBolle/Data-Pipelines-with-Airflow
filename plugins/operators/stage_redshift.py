# This module creates a table in Redshift, copies data from S3 into the table, and performs a quality check on the data.

from typing import List, Optional

from airflow.exceptions import AirflowException
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.utils.redshift import build_credentials_block
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """
    This operator extends the S3ToRedShiftOperator with a create table statement and quality checks. Please note that a
    large part of this code is directly copy-pasted from https://github.com/apache/airflow/blob/master/airflow/providers/amazon/aws/transfers/s3_to_redshift.py.
    """
    ui_color = '#358140'

    @apply_defaults
    def __init__(self,
                 create_table_sql: str,
                 schema: str,
                 table: str,
                 s3_bucket: str,
                 s3_key: str,
                 redshift_conn_id: str = 'redshift_default',
                 aws_conn_id: str = 'aws_default',
                 copy_options: Optional[List] = None,
                 *args, **kwargs) -> None:
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.create_table_sql = create_table_sql
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.copy_options = copy_options or []

    def _build_copy_query(self, credentials_block: str, copy_options: str) -> str:
        return f"""
                    COPY {self.schema}.{self.table}
                    FROM 's3://{self.s3_bucket}/{self.s3_key}'
                    with credentials
                    '{credentials_block}'
                    {copy_options};
        """

    def execute(self) -> None:
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_hook = S3Hook(aws_conn_id=self.aws_conn_id)
        credentials = s3_hook.get_credentials()
        credentials_block = build_credentials_block(credentials)
        copy_options = '\n\t\t\t'.join(self.copy_options)

        copy_statement = self._build_copy_query(credentials_block, copy_options)

        self.log.info("Creating the staging table...")
        postgres_hook.run(self.create_table_sql)
        self.log.info("Creating the staging table complete...")

        self.log.info('Executing COPY command...')
        postgres_hook.run(copy_statement)
        self.log.info("COPY command complete...")

        self.log.info("Logging the number of rows and files on S3 affected...")
        number_of_rows = postgres_hook.get_first(f"SELECT count(*) FROM {self.schema}.{self.table}")[0]
        number_of_keys_s3 = s3_hook.list_keys(bucket_name=self.s3_bucket, prefix=self.s3_key)

        self.log.info(f"{self.schema}.{self.table} has {number_of_rows} rows")
        self.log.info(f"{self.s3_bucket}/{self.s3_key} has {len(number_of_keys_s3)} files")

        self.log.info("Logging the number of rows and files on S3 affected complete...")
