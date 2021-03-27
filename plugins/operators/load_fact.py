# Module designed specifically for the songplays_table, hence no additional parameters


from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):
    """
    This operator allows for append only mode due to the fact that typical fact tables are very large in size.
    When you are using this operator for a new table you can use the create table statement though.
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
