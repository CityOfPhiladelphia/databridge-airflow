"""Defines a BatchGeocoderOperator to data geocode files in S3."""
from typing import Optional, List, Type

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperator


class BatchGeocoderOperator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to batch geocode data in S3."""

    @apply_defaults
    def __init__(self,
                 name: str,
                 input_file: str,
                 output_file: str,
                 query_fields: str,
                 ais_fields: str,
                 remove_fields: Optional[str] = None,
                 **kwargs):
        self.name = name
        self.input_file = input_file
        self.output_file = output_file
        self.query_fields = query_fields
        self.ais_fields = ais_fields
        self.remove_fields = remove_fields
        super().__init__(**kwargs)

    @property
    def _job_name(self) -> str:
        return 'batch_geocoder_{}'.format(self.name)

    @property
    def _job_definition(self) -> str:
        return 'batch-geocoder-airflow-{}'.format(self.ENVIRONMENT)

    @property
    def connection(self) -> Type:
        return BaseHook.get_connection('ais')

    @property
    def _command(self) -> List[str]:
        command = [
            'batch_geocoder',
            'ais',
            '--input-file={}'.format(self.input_file),
            '--output-file={}'.format(self.output_file),
            '--ais-url={}'.format(self.connection.host),
            '--ais-key={}'.format(self.connection.password),
            '--ais-user={}'.format(self.connection.login),
            '--query-fields={}'.format(self.query_fields),
            '--ais-fields={}'.format(self.ais_fields),
        ]
        if self.remove_fields:
            command.append('--remove-fields={}'.format(self.remove_fields))
        return command

    @property
    def _task_id(self) -> str:
        return 'batch_geocoder_{}'.format(self.name)
