""""
This module creates dags to extract data from databridge and load it to databridge2 and carto.
"""
from datetime import datetime, timedelta
import os
import yaml

from airflow import DAG

from slack_notify_plugin import SlackNotificationOperator
from databridge_operators import DataBridgeToS3Operator, S3ToDataBridge2Operator
from carto_operator import S3ToCartoOperator


def databridge_carto_dag_factory(
        table_schema,
        table_name,
        upload_to_carto,
        schedule_interval,
        select_users):

    dag_id = '{}__{}'.format(table_schema, table_name)

    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019, 10, 1, 0, 0, 0) - timedelta(hours=8),
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': 2 if 'PROD' in os.environ else 0
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

        if upload_to_carto:
            s3_to_carto = S3ToCartoOperator(
                table_schema=table_schema,
                table_name=table_name,
                select_users=select_users)

            databridge_to_s3 >> s3_to_carto

        globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

for department in os.listdir(os.path.join('dags', 'carto_dag_config')):
    for table_config_file in os.listdir(os.path.join('dags', 'carto_dag_config', department)):
        # Drop the file extension
        table_name = table_config_file.split('.')[0]

        with open(os.path.join('dags', 'carto_dag_config', department, table_config_file)) as f:
            yaml_data = yaml.safe_load(f.read())

            upload_to_carto = yaml_data.get('upload_to_carto')
            schedule_interval = yaml_data.get('schedule_interval')
            select_users = ','.join(yaml_data.get('carto_users'))

        databridge_carto_dag_factory(
            table_schema=department,
            table_name=table_name,
            upload_to_carto=upload_to_carto,
            schedule_interval=schedule_interval,
            select_users=select_users)
