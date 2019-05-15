"""Defines a KnackToS3Operator to extract data from Knack."""
from typing import Type, List, Union

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from abstract_batch_operator import PartialAWSBatchOperator


class KnackToS3Operator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to extract data from Knack to S3."""

    @apply_defaults
    def __init__(self, object_id, *args, **kwargs):
        self.object_id = object_id
        super(KnackToS3Operator, self).__init__(*args, **kwargs)

    @property
    def _job_name(self) -> str:
        return 'knack_to_s3_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'knack-airflow'

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
            self.object_id,
            '--s3_bucket={}'.format(self.S3_BUCKET),
            '--s3_key={}'.format(self.csv_s3_key),
        ]
        return command

    @property
    def _task_id(self) -> str:
        return 'knack_to_s3_{}_{}'.format(self.table_schema, self.table_name)
