"""Defines a AirtableToS3BatchOperator to extract data from Airtable to S3."""
from typing import Optional, List, Type

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperatorWithTable


class AirtableToS3BatchOperator(PartialAWSBatchOperatorWithTable):
    """Runs an AWS Batch Job to extract data from Airtable to S3."""

    @apply_defaults
    def __init__(self,
                 conn_id: str,
                 **kwargs):
        self.conn_id = conn_id
        super().__init__(**kwargs)

    @property
    def _job_name(self) -> str:
        return 'extract_airtable_{}'.format(self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'airtable-airflow-{}'.format(self.ENVIRONMENT)

    @property
    def connection(self) -> Type:
        return BaseHook.get_connection(self.conn_id)

    @property
    def _command(self) -> List[str]:
        command = [
            'extract-airtable',
            'extract-records',
            self.connection.login,
            self.connection.password,
            self.table_name,
            '--s3-bucket={}'.format(self.S3_BUCKET),
            '--s3-key={}'.format(self.csv_s3_key)
        ]
        return command

    @property
    def _task_id(self) -> str:
        return 'extract_airtable_{}'.format(self.table_name)
