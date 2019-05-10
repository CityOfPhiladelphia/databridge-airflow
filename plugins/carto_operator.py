"""Defines a S3ToCartoOperator to load data from S3 to Carto."""
from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from abstract_batch_operator import PartialAWSBatchOperator


class S3ToCartoOperator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to load data from S3 to Carto."""

    @apply_defaults
    def __init__(self, select_users, *args, **kwargs):
        self.select_users = select_users
        super(S3ToCartoOperator, self).__init__(*args, **kwargs)

    @property
    def _job_name(self):
        return 's3_to_carto_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self):
        return 'carto-db2-airflow-{}'.format(self.ENVIRONMENT)

    @property
    def connection(self):
        return BaseHook.get_connection('carto_phl')

    @property
    def _command(self):
        command = [
            'databridge_etl_tools',
            'cartoupdate',
            '--table_name={}'.format(self.table_name),
            '--connection_string={}'.format(self.connection.password),
            '--s3_bucket={}'.format(self.S3_BUCKET),
            '--json_schema_s3_key={}'.format(self.json_schema_s3_key),
            '--csv_s3_key={}'.format(self.csv_s3_key),
            "--select_users={}".format(self.select_users),
        ]
        return command

    @property
    def _task_id(self):
        return 's3_to_carto_{}_{}'.format(self.table_schema, self.table_name)