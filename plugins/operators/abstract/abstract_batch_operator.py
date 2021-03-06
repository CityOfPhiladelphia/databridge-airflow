"""Defines a PartialAWSBatchOperator to input default AWS Batch values."""

from abc import ABC, abstractmethod
import os
from typing import Dict

from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.utils.decorators import apply_defaults


class PartialAWSBatchOperator(AWSBatchOperator, ABC):
    """Sets default AWS Batch values."""

    ui_color = '#ededed'

    ENVIRONMENT = os.environ['ENVIRONMENT']
    AWS_REGION = 'us-east-1'
    S3_BUCKET = 'citygeo-airflow-databridge2'

    @apply_defaults
    def __init__(
            self,
            *args, **kwargs):

        super().__init__(
            job_name=self._job_name,
            job_definition=self._job_definition,
            job_queue=self._job_queue,
            region_name=self.AWS_REGION,
            overrides=self._overrides,
            task_id=self._task_id,
            *args, **kwargs)

    @property
    @abstractmethod
    def _job_name(self):
        pass

    @property
    @abstractmethod
    def _job_definition(self):
        pass

    @property
    def _job_queue(self) -> str:
        return 'airflow-{}'.format(self.ENVIRONMENT)

    @property
    @abstractmethod
    def _command(self):
        pass

    @property
    def _overrides(self) -> Dict[str, str]:
        return {'command': self._command}

    @property
    @abstractmethod
    def _task_id(self):
        pass

class PartialAWSBatchOperatorWithTable(PartialAWSBatchOperator):
    """Sets default AWS Batch values for common jobs that use a table schema and table name."""
    @apply_defaults
    def __init__(
            self,
            table_schema: str,
            table_name: str,
            *args, **kwargs):

        self.table_schema = table_schema
        self.table_name = table_name

        super().__init__()

    @property
    def json_schema_s3_key(self) -> str:
        return 'schemas/{}/{}.json'.format(self.table_schema, self.table_name)

    @property
    def csv_s3_key(self) -> str:
        return 'staging/{}/{}.csv'.format(self.table_schema, self.table_name)