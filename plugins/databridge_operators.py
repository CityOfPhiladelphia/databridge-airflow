from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.utils.decorators import apply_defaults


db_conn = BaseHook.get_connection('databridge')
db2_conn = BaseHook.get_connection('databridge2')
carto_conn = BaseHook.get_connection('carto_phl')

class DataBridgeToS3Operator(AWSBatchOperator):
    """
    Runs an AWS Batch Job to extract data from DataBridge to S3

    :param table_schema: schema name of the table in the source oracle database
    :type table_schema: string
    :param table_name: table name of the table in the source oracle database
    :type table_name: string
    """

    ui_color = '#ededed'
    
    @apply_defaults
    def __init__(
        self,
        table_schema,
        table_name,
        *args, **kwargs):
        super(DataBridgeToS3Operator, self).__init__(
            job_name='db_to_s3_{}_{}'.format(table_schema, table_name),
            job_definition='test_extract_and_load_to_databridge',
            job_queue='databridge-airflow',
            region_name='us-east-1',
            overrides={
                'command': [
                    'python3 /extract_and_load_to_databridge.py', 
                    'extract', 
                    'db_type={}'.format(db_conn.conn_type), 
                    'db_host={}'.format(db_conn.host), 
                    'db_user={}'.format(db_conn.login), 
                    'db_password={}'.format(db_conn.password), 
                    'db_name={}'.format(db_conn.extra), 
                    'db_port={}'.format(db_conn.port), 
                    'db_table_schema={}'.format(table_schema), 
                    'db_table_name={}'.format(table_name), 
                    's3_bucket=citygeo-airflow-databridge2',
                ],
            },
            task_id='db_to_s3_{}_{}'.format(table_schema, table_name),
            *args, **kwargs)

class S3ToDataBridge2Operator(AWSBatchOperator):
    """
    Runs an AWS Batch Job to load data from S3 to DataBridge2
    
    :param table_schema: schema name of the csv in S3
    :type table_schema: string
    :param table_name: table name of the csv in S3
    :type table_name: string
    """

    ui_color = '#ededed'
    
    @apply_defaults
    def __init__(
        self,
        table_schema,
        table_name,
        *args, **kwargs):
        super(S3ToDataBridge2Operator, self).__init__(
            job_name='s3_to_databridge2_{}_{}'.format(table_schema, table_name),
            job_definition='test_extract_and_load_to_databridge',
            job_queue='databridge-airflow',
            region_name='us-east-1',
            overrides={
                'command': [
                    'python3 /extract_and_load_to_databridge.py',
                    'write',
                    'db_type={}'.format(db2_conn.conn_type),
                    'db_host={}'.format(db2_conn.host),
                    'db_user={}'.format(db2_conn.login),
                    'db_password={}'.format(db2_conn.password),
                    'db_name={}'.format(db2_conn.extra),
                    'db_port={}'.format(db2_conn.port),
                    'db_table_schema={}'.format(table_schema),
                    'db_table_name={}'.format(table_name),
                    's3_bucket=citygeo-airflow-databridge2',
                ],
            },
            task_id='s3_to_databridge2_{}_{}'.format(table_schema, table_name),
            *args, **kwargs)

class S3ToCartoOperator(AWSBatchOperator):
    """
    Runs an AWS Batch Job to load data from S3 to Carto
    
    :param table_schema: schema name of the csv in S3
    :type table_schema: string
    :param table_name: table name of the csv in S3
    :type table_name: string
    """
    
    ui_color = '#ededed'
    
    @apply_defaults
    def __init__(
        self,
        table_schema,
        table_name,
        *args, **kwargs):
        super(S3ToCartoOperator, self).__init__(
            job_name='s3_to_carto_{}_{}'.format(table_schema, table_name),
            job_definition='test_extract_and_load_to_databridge',
            job_queue='databridge-airflow',
            region_name='us-east-1',
            overrides={
                'command': [
                    'python3 /extract_and_load_to_databridge.py',
                    'carto_update_table',
                    'carto_connection_string={}'.format(carto_conn.password),
                    's3_bucket=citygeo-airflow-databridge2',
                    'json_schema_file={}__{}.json'.format(table_schema, table_name),
                ],
            },
            task_id='s3_to_carto_{}_{}'.format(table_schema, table_name),
            *args, **kwargs)
