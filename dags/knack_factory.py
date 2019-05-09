# Date Last Updated: 5/9/2019
from datetime import datetime, timedelta
import os
import yaml

from airflow import DAG
from airflow.hooks.base_hook import BaseHook

from slack_notify_plugin import SlackNotificationOperator
from knack_operator import KnackToS3Operator
from carto_operator import S3ToCartoOperator


def knack_dag_factory(
    object_id,
    table_name,
    table_schema,
    upload_to_carto,
    schedule_interval,
    select_users,
    retries=0):

    dag_id = table_name

    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019, 5, 10, 0, 0, 0) - timedelta(hours=8),
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': retries
    }

    with DAG(
        dag_id=dag_id, 
        schedule_interval=schedule_interval,
        default_args=default_args,
        max_active_runs=1,
    ) as dag:

        knack_to_s3 = KnackToS3Operator(
                object_id=object_id,
                table_schema=table_schema, 
                table_name=table_name)

        knack_to_s3

        if upload_to_carto:
            s3_to_carto = S3ToCartoOperator(
                    table_schema=table_schema, 
                    table_name=table_name,
                    select_users=select_users)

            knack_to_s3 >> s3_to_carto

        globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

for yaml_file in os.listdir('dags/knack_dag_config'):
    table_name = yaml_file.split('.')[0]
    with open(os.path.join('dags/knack_dag_config', yaml_file)) as f:
        try:
            yaml_data = yaml.safe_load(f.read())
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.error('Failed to load {}'.format(yaml_file))
            raise e
        object_id = yaml_data.get('knack_object_id')
        table_schema = yaml_data.get('table_schema')
        upload_to_carto = yaml_data.get('upload_to_carto')
        schedule_interval = yaml_data.get('schedule_interval')
        select_users = ','.join(yaml_data.get('carto_users'))
    knack_dag_factory(object_id, table_name, table_schema, upload_to_carto, schedule_interval, select_users) 
