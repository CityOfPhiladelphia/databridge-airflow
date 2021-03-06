"""Defines a KnackToS3Operator to extract data from Knack."""
from typing import Type, List, Union

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperatorWithTable


class KnackToS3BatchOperator(PartialAWSBatchOperatorWithTable):
    """Runs an AWS Batch Job to extract data from Knack to S3."""

    @apply_defaults
    def __init__(self, conn_id: str, object_id: int, *args, **kwargs):
        self.conn_id = conn_id
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
        return BaseHook.get_connection(self.conn_id)

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
