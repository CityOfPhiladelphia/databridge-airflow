"""Defines an operator to load data from S3 to Postgres in an AWS Batch Job."""
from typing import List
import json

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperator


class S3ToPostgresBatchOperator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to load data from S3 to Postgres."""

    @apply_defaults
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @property
    def connection_string(self) -> str:
        db_conn = BaseHook.get_connection(self.conn_id)

        connection_string = 'postgresql://{login}:{password}@{host}:{port}/{db_name}'.format(
            login=db_conn.login,
            password=db_conn.password,
            host=db_conn.host,
            port=db_conn.port,
            db_name=json.loads(db_conn.extra)['db_name']
        )

        return connection_string

    @property
    def _table_schema(self) -> str:
        table_schema = self.table_schema

        # TODO: When the database migration is complete, this can be removed
        if table_schema.isdigit():
            table_schema = self.integer_to_word(table_schema)

        return table_schema

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

    @property
    def _job_name(self) -> str:
        return 's3_to_postgres_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'carto-db2-airflow-{}'.format(self.ENVIRONMENT)

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
        return 's3_to_postgres_batch_{}_{}'.format(self.table_schema, self.table_name)
