# Module designed to easily append data to a fact table


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    This operator takes care of inserting data into the fact table. You may only append data to an existing table since
    we expect the fact table to be too large in size to truncate-append.
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 insert_table_sql: str,
                 redshift_conn_id: str = 'redshift_default',
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.insert_table_sql = insert_table_sql
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("Inserting the data into the table...")
        postgres_hook.run(self.insert_table_sql)
        self.log.info("Inserting the data into the table completed...")
