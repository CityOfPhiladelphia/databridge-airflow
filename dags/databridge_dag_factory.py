""""
This module creates dags to extract data from databridge and load it to databridge2 and carto.
"""
from typing import Optional
from datetime import datetime, timedelta
import os
import yaml

from airflow import DAG

from airflow.operators.slack_notify_plugin import SlackNotificationOperator
from airflow.operators.databridge_plugin import DataBridgeToS3BatchOperator
from airflow.operators.carto_plugin import S3ToCartoBatchOperator, S3ToCartoBatchOperator


CARTO_PHL_CONN_ID = 'carto_phl'

def databridge_carto_dag_factory(
        table_schema: str,
        table_name: str,
        upload_to_carto: bool,
        schedule_interval: str,
        select_users: str,
        index_fields: Optional[str] = None) -> None:

    dag_id = '{}__{}'.format(table_schema, table_name)

    default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019, 5, 30, 0, 0, 0) - timedelta(hours=8),
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': 2 if os.environ['ENVIRONMENT'] == 'prod' else 0,
        'retry_delay': timedelta(minutes=5)
    }

    with DAG(
            dag_id=dag_id,
            schedule_interval=schedule_interval,
            default_args=default_args,
            max_active_runs=1,
    ) as dag:

        databridge_to_s3 = DataBridgeToS3BatchOperator(
            table_schema=table_schema,
            table_name=table_name)

        # s3_to_databridge2 = S3ToDataBridge2BatchOperator(
        #     table_schema=table_schema,
        #     table_name=table_name)

        # databridge_to_s3 >> s3_to_databridge2

        if upload_to_carto:
            s3_to_carto = S3ToCartoBatchOperator(
                conn_id=CARTO_PHL_CONN_ID,
                table_schema=table_schema,
                table_name=table_name,
                select_users=select_users,
                index_fields=index_fields,
                pool='carto')

            databridge_to_s3 >> s3_to_carto

        else:
            databridge_to_s3

        globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

for department in os.listdir(os.path.join('dags', 'databridge_dag_config')):
    for table_config_file in os.listdir(os.path.join('dags', 'databridge_dag_config', department)):
        # Drop the file extension
        table_name = table_config_file.split('.')[0]

        with open(os.path.join('dags', 'databridge_dag_config', department, table_config_file)) as f:
            yaml_data = yaml.safe_load(f.read())

            upload_to_carto = yaml_data.get('upload_to_carto')
            schedule_interval = yaml_data.get('schedule_interval')
            select_users = ','.join(yaml_data.get('carto_users'))
            index_list = yaml_data.get('indexes', None)

            if index_list:
                index_fields = ','.join(index_list)
            else:
                index_fields = None

        databridge_carto_dag_factory(
            table_schema=department,
            table_name=table_name,
            upload_to_carto=upload_to_carto,
            schedule_interval=schedule_interval,
            select_users=select_users,
            index_fields=index_fields)
