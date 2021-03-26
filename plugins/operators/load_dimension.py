# Module designed to easily append or truncate-append data to dimension tables

# Do you want to separate the DROP TABLE and INSERT table statements, it is probably saver.
# The data quality checks are part of the load table modules since I want to check them directly, perhaps
# for overview and debugging it makes more sense to call them separately in the dag? you can keep the queries
# separated in data_quality


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.sql import SQLCheckOperator


class LoadDimensionOperator(BaseOperator):
    """The default behaviour is to truncate-append the data"""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 create_table_sql: str,
                 data_quality_sql: str,
                 insert_table_sql: str,
                 schema: str,
                 table: str,
                 truncate: bool = False,
                 redshift_conn_id: str = 'redshift_default',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.create_table_sql = create_table_sql
        self.data_quality_sql = data_quality_sql
        self.insert_table_sql = insert_table_sql
        self.redshift_conn_id = redshift_conn_id
        self.truncate = truncate

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info("Truncating the table...")
            postgres_hook.run(self.create_table_sql)
            self.log.info("Truncating the table completed...")

        self.log.info("Inserting the data into the table...")
        postgres_hook.run(self.insert_table_sql)
        self.log.info("Inserting the data into the table completed...")

        if self.data_quality_sql:
            self.log.info("Checking the quality of the inserted data...")
            SQLCheckOperator(task_id='data quality check', sql=self.data_quality_sql)
            self.log.info("Checking the quality of the inserted data completed...")
