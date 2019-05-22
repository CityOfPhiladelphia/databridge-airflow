"""Defines operators to extract and load data to and from databridge / databridge2."""
from typing import List
import json

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults
from airflow.plugins_manager import AirflowPlugin

import cx_Oracle

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperator


class DataBridgeToS3Operator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to extract data from DataBridge to S3."""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(DataBridgeToS3Operator, self).__init__(*args, **kwargs)

    @property
    def _job_name(self) -> str:
        return 'db_to_s3_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'carto-db2-airflow'

    @property
    def connection_string(self) -> str:
        db_conn = BaseHook.get_connection('databridge')

        connection_string = '{}/{}@{}'.format(
            db_conn.login,
            db_conn.password,
            cx_Oracle.makedsn(db_conn.host,
                              db_conn.port,
                              json.loads(db_conn.extra)['db_name'])
        )

        return connection_string

    @property
    def _command(self) -> List[str]:
        command = [
            'databridge_etl_tools',
            'extract',
            '--table_name={}'.format(self.table_name),
            '--table_schema=gis_{}'.format(self.table_schema),
            '--connection_string={}'.format(self.connection_string),
            '--s3_bucket={}'.format(self.S3_BUCKET),
            '--s3_key={}'.format(self.csv_s3_key),
        ]
        return command

    @property
    def _task_id(self) -> str:
        return 'db_to_s3_{}_{}'.format(self.table_schema, self.table_name)

class S3ToDataBridge2Operator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to load data from S3 to DataBridge2."""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super(S3ToDataBridge2Operator, self).__init__(*args, **kwargs)

    @property
    def _table_schema(self) -> str:
        table_schema = self.table_schema

        # TODO: When the database migration is complete, this can be removed
        if table_schema.isdigit():
            table_schema = self.integer_to_word(table_schema)

        return table_schema

    @property
    def _job_name(self) -> str:
        return 's3_to_databridge2_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'carto-db2-airflow'

    @property
    def connection_string(self) -> str:
        db2_conn = BaseHook.get_connection('databridge2')

        connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
            db2_conn.login,
            db2_conn.password,
            db2_conn.host,
            db2_conn.port,
            json.loads(db2_conn.extra)['db_name'])

        return connection_string

    @property
    def _command(self) -> List[str]:
        command = [
            'databridge_etl_tools',
            'load',
            '--table_name={}'.format(self.table_name),
            '--table_schema={}'.format(self._table_schema),
            '--connection_string={}'.format(self.connection_string),
            '--s3_bucket={}'.format(self.S3_BUCKET),
            '--json_schema_s3_key={}'.format(self.json_schema_s3_key),
            '--csv_s3_key={}'.format(self.csv_s3_key),
        ]
        return command

    @property
    def _task_id(self) -> str:
        return 's3_to_databridge2_{}_{}'.format(self.table_schema, self.table_name)

    # TODO: When the database migration is complete, this can be removed
    @staticmethod
    def integer_to_word(integer: int) -> str:
        '''Converts integers to words, ie. 311 -> threeoneone '''
        INT_WORD_MAP = {
            '1': 'one',
            '2': 'two',
            '3': 'three',
            '4': 'four',
            '5': 'five',
            '6': 'six',
            '7': 'seven',
            '8': 'eight',
            '9': 'nine',
        }

        integer_as_string = str(integer)
        result = ''

        for letter in integer_as_string:
            spelled_out_integer = INT_WORD_MAP.get(letter)
            result += spelled_out_integer

        return result
