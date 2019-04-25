# Date Last Updated: 4/24/2019
from datetime import datetime
import os
import yaml

from airflow import DAG
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.hooks.base_hook import BaseHook

from slack_notify_plugin import SlackNotificationOperator


TEST = 'True'

def databridge_carto_dag_factory(
    table_schema,
    table_name,
    upload_to_carto,
    schedule_interval,
    retries=0):

    db_conn = BaseHook.get_connection('databridge')
    db2_conn = BaseHook.get_connection('databridge2')
    carto_conn = BaseHook.get_connection('carto_phl')

    dag_id = '{}__{}'.format(table_schema.split('_')[1], table_name)

    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019, 4, 20, 0, 0, 0),
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': retries
    }
    
    with DAG(
        dag_id=dag_id, 
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
            retries=3,
        )

        if upload_to_carto:
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
                        'yaml_file={}__{}.json'.format(table_schema, table_name),
                    ],
                },
                task_id='s3_to_carto_{}_{}'.format(table_schema, table_name),
            )
        #    databridge_to_s3 >> [s3_to_databridge2, s3_to_carto]
            databridge_to_s3 >> s3_to_carto
        else:
        #    databridge_to_s3 >> s3_to_databridge2
            databridge_to_s3

        globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

for yaml_file in os.listdir('dags/dag_config'):
    schema_name = yaml_file.split('__')[0]
    table_name = yaml_file.split('__')[1].split('.')[0]
    with open(os.path.join('dags/dag_config', yaml_file)) as f:
        try:
            yaml_data = yaml.safe_load(f.read())
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error('Failed to load {}'.format(yaml_file))
            raise e
        upload_to_carto = yaml_data.get('upload_to_carto')
        schedule_interval = yaml_data.get('schedule_interval')
    databridge_carto_dag_factory(schema_name, table_name, upload_to_carto, schedule_interval)
