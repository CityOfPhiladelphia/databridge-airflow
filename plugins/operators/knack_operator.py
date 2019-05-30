"""Defines a KnackToS3Operator to extract data from Knack."""
from typing import Type, List, Union
import json

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperator
from operators.abstract.abstract_lambda_operator import PartialAWSLambdaOperator


class KnackToS3BatchOperator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to extract data from Knack to S3."""

    @apply_defaults
    def __init__(self, object_id, *args, **kwargs):
        self.object_id = object_id
        super().__init__(*args, **kwargs)

    @property
    def _job_name(self) -> str:
        return 'knack_to_s3_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'knack-airflow-{}'.format(self.ENVIRONMENT)

    @property
    def connection(self) -> Type:
        return BaseHook.get_connection('knack')

    @property
    def _command(self) -> List[Union[str, int]]:
        command = [
            'extract-knack',
            'extract-records',
            self.connection.login,
            self.connection.password,
            str(self.object_id),
            '--s3_bucket={}'.format(self.S3_BUCKET),
            '--s3_key={}'.format(self.csv_s3_key),
        ]
        return command

    @property
    def _task_id(self) -> str:
        return 'knack_to_s3_batch_{}_{}'.format(self.table_schema, self.table_name)

class KnackToS3LambdaOperator(PartialAWSLambdaOperator):
    """Runs a AWS Lambda Function to extract data from Knack to S3."""

    function_name = 'extract-knack'

    def __init__(self, object_id, *args, **kwargs):
        self.object_id = object_id
        super().__init__(*args, **kwargs)

    @property
    def connection(self) -> Type:
        return BaseHook.get_connection('knack')

    @property
    def _task_id(self) -> str:
        return 'knack_to_s3_lambda_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def payload(self) -> Type:
        return json.dumps({
            'command_name': 'extract-records',
            'app-id': self.connection.login,
            'app-key': self.connection.password,
            'object-id': str(self.object_id),
            's3_bucket': self.S3_BUCKET,
            's3_key': self.csv_s3_key
        })
