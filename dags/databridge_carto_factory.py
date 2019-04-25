# Date Last Updated: 4/24/2019
from datetime import datetime
import os
import yaml

from airflow import DAG
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.hooks.base_hook import BaseHook

from slack_notify_plugin import SlackNotificationOperator
from databridge_operators import (
    DataBridgeToS3Operator,
    S3ToDataBridge2Operator,
    S3ToCartoOperator,
)


def databridge_carto_dag_factory(
    table_schema,
    table_name,
    upload_to_carto,
    schedule_interval,
    retries=0):

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

        databridge_to_s3 = DataBridgeToS3Operator(
                table_schema=table_schema, 
                table_name=table_name)
        s3_to_databridge2 = S3ToDataBridge2Operator(
                table_schema=table_schema, 
                table_name=table_name)

        databridge_to_s3 >> s3_to_databridge2

#        if upload_to_carto:
#            s3_to_carto = S3ToCartoOperator(
#                    table_schema=table_schema, 
#                    table_name=table_name)
#            s3_to_databridge2 >> s3_to_carto

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
