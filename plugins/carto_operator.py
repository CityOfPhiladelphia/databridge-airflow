import os

from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.utils.decorators import apply_defaults

ENVIRONMENT = os.environ['ENVIRONMENT']

carto_conn = BaseHook.get_connection('carto_phl')

class S3ToCartoOperator(AWSBatchOperator):
    """
    Runs an AWS Batch Job to load data from S3 to Carto
    
    :param table_schema: schema name of the csv in S3
    :type table_schema: string
    :param table_name: table name of the csv in S3
    :type table_name: string
    :param select_users: carto users to grant select to (comma separated string)
    :type seleect_users string
    """
    
    ui_color = '#ededed'
    
    @apply_defaults
    def __init__(
        self,
        table_schema,
        table_name,
        select_users,
        *args, **kwargs):
        self.table_schema = table_schema
        super(S3ToCartoOperator, self).__init__(
            job_name='s3_to_carto_{}_{}'.format(table_schema, table_name),
            job_definition='carto-db2-airflow-{}'.format(ENVIRONMENT),
            job_queue='airflow-{}'.format(ENVIRONMENT),
            region_name='us-east-1',
            overrides={
                'command': [
                    'databridge_etl_tools',
                    'cartoupdate',
                    '--table_name={}'.format(table_name),
                    '--connection_string={}'.format(carto_conn.password),
                    '--s3_bucket=citygeo-airflow-databridge2',
                    '--json_schema_s3_key=schemas/{}__{}.json'.format(table_schema, table_name),
                    '--csv_s3_key=staging/{}/{}.csv'.format(
                        self.carto_table_schema,
                        table_name),
                    "--select_users={}".format(select_users),
                ],
            },
            task_id='s3_to_carto_{}_{}'.format(table_schema, table_name),
            *args, **kwargs)

    @property
    def carto_table_schema(self):
        if '_' in self.table_schema:
            # Remove 'gis_' from the schema
            table_schema = self.table_schema.split('_', 1)[1]
        else:
            table_schema = self.table_schema
        return table_schema