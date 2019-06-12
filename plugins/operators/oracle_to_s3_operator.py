"""Defines an operator to extract data from an Oracle database and load it to S3 in an AWS Batch Job."""
from typing import List
import json

from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

import cx_Oracle

from operators.abstract.abstract_batch_operator import PartialAWSBatchOperator


class OracleToS3BatchOperator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to extract data from Oracle to S3."""

    @apply_defaults
    def __init__(self, conn_id: str, *args, **kwargs):
        self.conn_id = conn_id
        super().__init__(*args, **kwargs)
        
    @property
    def connection_string(self) -> str:
        db_conn = BaseHook.get_connection(self.conn_id)

        connection_string = '{}/{}@{}'.format(
            db_conn.login,
            db_conn.password,
            cx_Oracle.makedsn(db_conn.host,
                              db_conn.port,
                              json.loads(db_conn.extra)['db_name'])
        )

        return connection_string

    @property
    def _job_name(self) -> str:
        return 'oracle_to_s3_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self) -> str:
        return 'carto-db2-airflow-{}'.format(self.ENVIRONMENT)

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
        return 'oracle_to_s3_batch_{}_{}'.format(self.table_schema, self.table_name)
