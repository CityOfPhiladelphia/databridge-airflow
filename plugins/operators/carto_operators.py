"""Defines a S3ToCartoOperator to load data from S3 to Carto."""
from typing import Optional, List, Type
import json
import base64

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperator


class S3ToCartoBatchOperator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to load data from S3 to Carto."""

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 select_users: str,
                 index_fields: Optional[str] = None,
                 **kwargs):
        self.conn_id = conn_id
        self.select_users = select_users
        self.index_fields = index_fields
        super().__init__(**kwargs)

    @property
    def _job_name(self) -> str:
        return 's3_to_carto_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'carto-db2-airflow-{}'.format(self.ENVIRONMENT)

    @property
    def connection(self) -> Type:
        return BaseHook.get_connection(self.conn_id)

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
        return 's3_to_carto_batch_{}_{}'.format(self.table_schema, self.table_name)
