""""
This module creates dags to extract data from knack and load it to carto.
"""
from typing import List
from datetime import datetime, timedelta
import os
import yaml

from airflow import DAG

from airflow.operators.slack_notify_plugin import SlackNotificationOperator
from airflow.operators.knack_plugin import KnackToS3BatchOperator
from airflow.operators.carto_plugin import S3ToCartoBatchOperator


def knack_dag_factory(
        knack_conn_id: str,
        carto_conn_id: str,
        object_id: int,
        table_name: str,
        table_schema: str,
        upload_to_carto: bool,
        schedule_interval: str,
        select_users: str,
        environment: str) -> None:

    dag_id = '{}__{}__{}'.format(table_schema, table_name, environment)

    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019, 5, 30, 0, 0, 0) - timedelta(hours=8),
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': 2 if os.environ['ENVIRONMENT'] == 'PROD' else 0,
        'retry_delay': timedelta(minutes=5)
    }

    with DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            max_active_runs=1,
    ) as dag:

        if 'test' in knack_conn_id:
            table_schema += '_test'

        knack_to_s3 = KnackToS3BatchOperator(
            conn_id=knack_conn_id,
            object_id=object_id,
            table_schema=table_schema,
            table_name=table_name)

        knack_to_s3

        if upload_to_carto:
            s3_to_carto = S3ToCartoBatchOperator(
                conn_id=carto_conn_id,
                table_schema=table_schema,
                table_name=table_name,
                select_users=select_users,
                pool='carto')

            knack_to_s3 >> s3_to_carto

        globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

for environment in os.listdir(os.path.join('dags', 'knack_dag_config')):
    for department in os.listdir(os.path.join('dags', 'knack_dag_config', environment)):
        for table_config_file in os.listdir(os.path.join('dags', 'knack_dag_config', environment, department)):
            # Drop the file extension
            table_name = table_config_file.split('.')[0]

            with open(os.path.join('dags', 'knack_dag_config', environment, department, table_config_file)) as f:
                yaml_data = yaml.safe_load(f.read())

                knack_conn_id = yaml_data.get('knack_conn_id')
                carto_conn_id = yaml_data.get('carto_conn_id')
                object_id = int(yaml_data.get('knack_object_id'))
                upload_to_carto = yaml_data.get('upload_to_carto')
                schedule_interval = yaml_data.get('schedule_interval')
                select_users = ','.join(yaml_data.get('carto_users'))

            knack_dag_factory(
                knack_conn_id=knack_conn_id,
                carto_conn_id=carto_conn_id,
                object_id=object_id,
                table_name=table_name,
                table_schema=department,
                upload_to_carto=upload_to_carto,
                schedule_interval=schedule_interval,
                select_users=select_users,
                environment=environment)
