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
    def __init__(self, select_users: str, index_fields: Optional[str] = None, *args, **kwargs):
        self.select_users = select_users
        self.index_fields = index_fields
        super().__init__(*args, **kwargs)

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

class S3ToCartoLambdaOperator(BaseOperator):
    """Runs an AWS Lambda Function to load data from S3 to Carto."""

    ui_color = '#f4b042'

    @apply_defaults
    def __init__(
        self, 
        table_name: str, 
        table_schema: str, 
        select_users: str,
        *args, **kwargs):

        self.table_name = table_name
        self.table_schema = table_schema
        self.select_users = select_users

        super().__init__(task_id=self._task_id, *args, **kwargs)

        self.hook = self.get_hook()

    @property
    def json_schema_s3_key(self) -> str:
        return 'schemas/{}/{}.json'.format(self.table_schema, self.table_name)

    @property
    def csv_s3_key(self) -> str:
        return 'staging/{}/{}.csv'.format(self.table_schema, self.table_name)

    @property
    def connection(self) -> Type:
        return BaseHook.get_connection('carto_phl')
    
    @property
    def _task_id(self) -> str:
        return 's3_to_carto_lambda_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def payload(self) -> Type:
        return json.dumps({
            'command_name': 'cartoupdate',
            'table_name': self.table_name,
            'connection_string': self.connection.password,
            's3_bucket': 'citygeo-airflow-databridge2',
            'json_schema_s3_key': self.json_schema_s3_key,
            'csv_s3_key': self.csv_s3_key,
            'select_users': self.select_users
        })

    def get_hook(self):
        return AwsLambdaHook(
            function_name='databridge-etl-tools',
            region_name='us-east-1',
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

        if response['StatusCode'] == 200:
            self.log.info('{} completed successfully!'.format(self.hook.function_name))
        else:
            self.log.info(
                '{} failed with status code {}'.format(
                    self.hook.function_name, response
                )
            )