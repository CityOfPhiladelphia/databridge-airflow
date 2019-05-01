from airflow.hooks.base_hook import BaseHook
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.utils.decorators import apply_defaults

import cx_Oracle


db_conn = BaseHook.get_connection('databridge')
db_connection_string = '{}/{}@{}'.format(
        db_conn.login,
        db_conn.password,
        cx_Oracle.makedsn(db_conn.host,
                          db_conn.port,
                          db_conn.extra)
        )
db2_conn = BaseHook.get_connection('databridge2')
db2_connection_string = 'postgresql://{}:{}@{}:{}/{}'.format(
        db2_conn.login,
        db2_conn.password,
        db2_conn.host,
        db2_conn.port,
        db2_conn.extra)
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
            job_definition='extract_and_load_to_databridge',
            job_queue='databridge-airflow2',
            region_name='us-east-1',
            overrides={
                'command': [
                    'databridge_etl_tools', 
                    'extract', 
                    '--table_name={}'.format(table_name),
                    '--table_schema={}'.format(table_schema), 
                    '--connection_string={}'.format(db_connection_string),
                    '--s3_bucket=citygeo-airflow-databridge2',
                    '--s3_key=staging/{}/{}.csv'.format(
                        table_schema.split('_')[1],
                        table_name),
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
            job_definition='extract_and_load_to_databridge',
            job_queue='databridge-airflow2',
            region_name='us-east-1',
            overrides={
                'command': [
                    'databridge_etl_tools',
                    'load',
                    '--table_name=databridge_{}'.format(table_name),
                    '--table_schema={}'.format(table_schema.split('_')[1]),
                    '--connection_string={}'.format(db2_connection_string),
                    '--s3_bucket=citygeo-airflow-databridge2',
                    '--json_schema_s3_key=schemas/{}__{}.json'.format(
                        table_schema,
                        table_name),
                    '--csv_s3_key=staging/{}/{}.csv'.format(
                        table_schema.split('_')[1],
                        table_name),
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
    :param select_users: carto users to grant select to (comma separated string)
    :type seleect_users string
    """
    
    ui_color = '#ededed'
    
    @apply_defaults
    def __init__(
        self,
        table_schema,
        table_name,
        select_users,
        *args, **kwargs):
        super(S3ToCartoOperator, self).__init__(
            job_name='s3_to_carto_{}_{}'.format(table_schema, table_name),
            job_definition='extract_and_load_to_databridge',
            job_queue='databridge-airflow2',
            region_name='us-east-1',
            overrides={
                'command': [
                    'databridge_etl_tools',
                    'cartoupdate',
                    '--table_name={}'.format(table_name),
                    '--connection_string={}'.format(carto_conn.password),
                    '--s3_bucket=citygeo-airflow-databridge2',
                    '--json_schema_s3_key=schemas/{}__{}.json'.format(table_schema, table_name),
                    '--csv_s3_key=staging/{}/{}.csv'.format(
                        table_schema.split('_')[1],
                        table_name),
                    "--select_users={}".format(select_users),
                ],
            },
            task_id='s3_to_carto_{}_{}'.format(table_schema, table_name),
            *args, **kwargs)
