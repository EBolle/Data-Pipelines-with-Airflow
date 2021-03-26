# Module designed specifically for the songplays_table, hence no additional parameters


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.operators.sql import SQLCheckOperator


class LoadFactOperator(BaseOperator):
    """
    This operator allows for append only mode due to the fact that typical fact tables are very large in size.
    When you are using this operator for a new table you can use the create table statement though.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 create_table_sql: str,
                 data_quality_sql: str,
                 insert_table_sql: str,
                 schema: str,
                 table: str,
                 redshift_conn_id: str = 'redshift_default',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.create_table_sql = create_table_sql
        self.data_quality_sql = data_quality_sql
        self.insert_table_sql = insert_table_sql
        self.schema = schema
        self.table = table
        self.redshift_conn_id = redshift_conn_id

    # Add check to make sure the target table is empty, if so, raise value error if create_table is not null.
    def _check_table_exist(self):
        pass

    def execute(self, context):
        self._check_table_exist

        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_table_sql:
            self.log.info("Creating the table...")
            postgres_hook.run(self.create_table_sql)
            self.log.info("Creating the table completed...")

        self.log.info("Inserting the data into the table...")
        postgres_hook.run(self.insert_table_sql)
        self.log.info("Inserting the data into the table completed...")

        if self.data_quality_sql:
            self.log.info("Checking the quality of the inserted data...")
            SQLCheckOperator(task_id='data quality check', sql=self.data_quality_sql)
            self.log.info("Checking the quality of the inserted data completed...")
