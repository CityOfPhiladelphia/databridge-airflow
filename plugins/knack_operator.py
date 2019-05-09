from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.utils.decorators import apply_defaults


knack_conn = BaseHook.get_connection('knack')

class KnackToS3Operator(AWSBatchOperator):
    """
    Runs an AWS Batch Job to extract data from Knack to S3

    :param object_id: knack object_id
    :type object_id: int
    :param table_schema: schema name in the S3 bucket
    :type table_schema: string
    :param table_name: table name in the S3 bucket
    :type table_name: string
    """

    ui_color = '#ededed'
    
    @apply_defaults
    def __init__(
        self,
        object_id,
        table_schema,
        table_name,
        *args, **kwargs):
        super(KnackToS3Operator, self).__init__(
            job_name='knack_to_s3_{}_{}'.format(table_schema, table_name),
            job_definition='extract_knack',
            job_queue='extract_knack',
            region_name='us-east-1',
            overrides={
                'command': [
                    'extract-knack', 
                    'extract-records',
                    knack_conn.login,
                    knack_conn.password,
                    object_id,
                    '--s3_bucket=citygeo-airflow-databridge2',
                    '--s3_key=staging/{}/{}.csv'.format(
                        table_schema,
                        table_name),
                ],
            },
            task_id='knack_to_s3_{}'.format(table_name),
            *args, **kwargs)