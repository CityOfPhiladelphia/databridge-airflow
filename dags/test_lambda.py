from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.slack_notify_plugin import SlackNotificationOperator
from airflow.operators.carto_plugin import S3ToCartoLambdaOperator


dag_id = '0_test_lambda'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 15, 0, 0, 0) - timedelta(hours=8),
    'on_success_callback': SlackNotificationOperator.success,
    'on_failure_callback': SlackNotificationOperator.failed,
    'retries': 0
}

with DAG(
        dag_id=dag_id,
        schedule_interval=None,
        default_args=default_args,
        max_active_runs=1,
) as dag:

    s3_to_carto_lambda = S3ToCartoLambdaOperator(
        table_name='li_imm_dang',
        table_schema='lni',
        select_users='publicuser,tileuser'
    )