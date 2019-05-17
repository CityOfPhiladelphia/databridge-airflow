"""Defines a S3ToCartoOperator to load data from S3 to Carto."""
from typing import Optional, List, Type

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from abstract_batch_operator import PartialAWSBatchOperator


class S3ToCartoOperator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to load data from S3 to Carto."""

    @apply_defaults
    def __init__(self, select_users: str, index_fields: Optional[str] = None, *args, **kwargs):
        self.select_users = select_users
        self.index_fields = index_fields
        super(S3ToCartoOperator, self).__init__(*args, **kwargs)

    @property
    def _job_name(self) -> str:
        return 's3_to_carto_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'carto-db2-airflow'

    @property
    def connection(self) -> Type:
        return BaseHook.get_connection('carto_phl')

    @property
    def _command(self) -> List[str]:
        command = [
            'databridge_etl_tools',
            'cartoupdate',
            '--table_name={}'.format(self.table_name),
            '--connection_string={}'.format(self.connection.password),
            '--s3_bucket={}'.format(self.S3_BUCKET),
            '--json_schema_s3_key={}'.format(self.json_schema_s3_key),
            '--csv_s3_key={}'.format(self.csv_s3_key),
            "--select_users={}".format(self.select_users)
        ]
        if self.index_fields:
            command.append('--index_fields={}'.format(self.index_fields))
        return command

    @property
    def _task_id(self) -> str:
        return 's3_to_carto_{}_{}'.format(self.table_schema, self.table_name)

class CartoPlugin(AirflowPlugin):
    name = 'carto_plugin'
    operators = [S3ToCartoOperator]