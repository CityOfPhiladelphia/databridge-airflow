"""Defines a PartialAWSLambdaOperator to input default AWS Lambda values."""

from abc import ABC, abstractmethod
import json
import base64

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.contrib.hooks.aws_lambda_hook import AwsLambdaHook


class LambdaFailedException(Exception): 
    pass

class PartialAWSLambdaOperator(BaseOperator, ABC):
    """Sets default AWS Lambda values."""

    ui_color = '#f4b042'

    AWS_REGION = 'us-east-1'
    S3_BUCKET = 'citygeo-airflow-databridge2'

    @apply_defaults
    def __init__(
            self,
            table_name: str,
            table_schema: str,
            *args, **kwargs):

        self.table_name = table_name
        self.table_schema = table_schema
        self.hook = self.get_hook()

        super().__init__(task_id=self._task_id, *args, **kwargs)

    @property
    def json_schema_s3_key(self) -> str:
        return 'schemas/{}/{}.json'.format(self.table_schema, self.table_name)

    @property
    def csv_s3_key(self) -> str:
        return 'staging/{}/{}.csv'.format(self.table_schema, self.table_name)
    
    @property
    @abstractmethod
    def _task_id(self):
        pass

    @property
    @abstractmethod
    def payload(self):
        pass

    def get_hook(self):
        return AwsLambdaHook(
            function_name=self.function_name,
            region_name=self.AWS_REGION,
            log_type='Tail',
            invocation_type='RequestResponse'
        )

    def execute(self, context):
        self.log.info(
            'Running AWS Lambda Function - Function name: {} - Payload: {}'.format(
                self.hook.function_name, self.payload
            )
        )

        response = self.hook.invoke_lambda(self.payload)

        # Returned logs are base64 encoded
        self.log.info('LogResult: {}'.format(base64.b64decode(response['LogResult'])))

        if 'FunctionError' in response:
            self.log.error('{} failed!'.format(self.hook.function_name))
            raise LambdaFailedException
        
        if response['StatusCode'] == 200:
            self.log.info('{} completed successfully!'.format(self.hook.function_name))
        else:
            self.log.info(
                '{} failed due to a network issue with status code {}'.format(
                    self.hook.function_name, response
                )
            )
