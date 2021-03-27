# Module designed to easily append or truncate-append data to dimension tables


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    """The default behaviour is to truncate-append the data"""

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 insert_table_sql: str,
                 schema: str,
                 table: str,
                 truncate: bool = False,
                 redshift_conn_id: str = 'redshift_default',
                 *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.insert_table_sql = insert_table_sql
        self.schema = schema
        self.table = table
        self.truncate = truncate
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate:
            self.log.info(f"Truncating {self.schema}.{self.table}...")
            postgres_hook.run(f"DELETE FROM {self.schema}.{self.table}")
            self.log.info(f"Truncating {self.schema}.{self.table} completed...")

        self.log.info(f"Inserting the data into {self.schema}.{self.table}...")
        sql = f"""
        INSERT INTO {self.schema}.{self.table}
        {self.insert_table_sql}
        """
        postgres_hook.run(sql)
        self.log.info(f"Inserting the data into {self.schema}.{self.table} completed...")
