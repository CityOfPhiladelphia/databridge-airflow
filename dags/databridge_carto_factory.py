from datetime import datetime, timedelta
import os
import json

from airflow import DAG
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.hooks.base_hook import BaseHook
from airflow.models import Variable

from slack_notify_plugin import SlackNotificationOperator


TEST = 'True'

def databridge_carto_dag_factory(
    table_schema,
    table_name,
    carto=True,
    schedule_interval=None,
    retries=0):

    db_conn = BaseHook.get_connection('databridge')
    db2_conn = BaseHook.get_connection('databridge2')
    carto_conn = BaseHook.get_connection('carto_phl')

    dag_id = '{}__{}'.format(table_schema.split('_')[1], table_name)

    default_args = {
        'owner': 'airflow',
        'on_success_callback': SlackNotificationOperator.success,
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': retries
    }
    
    with DAG(
        dag_id=dag_id, 
        start_date=datetime.now() - timedelta(days=1),
        schedule_interval=schedule_interval,
        default_args=default_args,
        max_active_runs=1,
    ) as dag:

        databridge_to_s3 = AWSBatchOperator(
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
        )

        s3_to_databridge2 = AWSBatchOperator(
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
        )

        if carto:
            s3_to_carto = AWSBatchOperator(
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
                    'environment': [
                        {
                            'name': 'TEST',
                            'value': TEST,
                        },
                    ],
                },
                task_id='s3_to_carto_{}_{}'.format(table_schema, table_name),
            )
            databridge_to_s3 >> [s3_to_databridge2, s3_to_carto]
        else:
            databridge_to_s3 >> s3_to_databridge2

        globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

for json_schema_file in os.listdir('schemas'):
    schema_name = json_schema_file.split('__')[0]
    table_name = json_schema_file.split('__')[1].split('.')[0]
    with open(os.path.join('schemas', json_schema_file)) as f:
        json_data = json.loads(f.read())
        carto = json_data.get('carto_table_name', True)
    databridge_carto_dag_factory(schema_name, table_name, carto)
