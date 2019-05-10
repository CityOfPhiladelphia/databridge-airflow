from airflow.hooks.base_hook import BaseHook
from airflow.utils.decorators import apply_defaults

from abstract_batch_operator import PartialAWSBatchOperator


class KnackToS3Operator(PartialAWSBatchOperator):
    """Runs an AWS Batch Job to extract data from Knack to S3."""

    @apply_defaults
    def __init__(self, object_id, *args, **kwargs):
        self.object_id = str(object_id) # Need to cast or Batch throws an error
        super(KnackToS3Operator, self).__init__(*args, **kwargs)

    @property
    def _job_name(self):
        return 'knack_to_s3_{}_{}'.format(self.table_schema, self.table_name)

    @property
    def _job_definition(self):
        return 'knack-airflow-{}'.format(self.ENVIRONMENT)

    @property
    def connection(self):
        return BaseHook.get_connection('knack')

    @property
    def _command(self):
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
    def _task_id(self):
        return 'knack_to_s3_{}_{}'.format(self.table_schema, self.table_name)
