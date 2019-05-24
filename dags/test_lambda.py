from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.slack_notify_plugin import SlackNotificationOperator
from airflow.operators.carto_plugin import S3ToCartoLambdaOperator
from airflow.operators.knack_plugin import KnackToS3LambdaOperator
from airflow.operators.databridge_plugin import DataBridgeToS3LambdaOperator, S3ToDataBridge2LambdaOperator


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 15, 0, 0, 0) - timedelta(hours=8),
    'on_success_callback': SlackNotificationOperator.success,
    'on_failure_callback': SlackNotificationOperator.failed,
    'retries': 0
}

with DAG(
        dag_id='0_test_s3_to_carto_lambda',
        schedule_interval=None,
        default_args=default_args,
        max_active_runs=1,
) as dag:

    s3_to_carto_lambda = S3ToCartoLambdaOperator(
        table_name='li_imm_dang',
        table_schema='lni',
        select_users='publicuser,tileuser'
    )

# with DAG(
#         dag_id='0_test_knack_to_s3_lambda',
#         schedule_interval=None,
#         default_args=default_args,
#         max_active_runs=1,
# ) as dag:

#     knack_to_s3_lambda = KnackToS3LambdaOperator(
#         table_name='li_imm_dang',
#         table_schema='lni',
#         select_users='publicuser,tileuser'
#     )

with DAG(
        dag_id='0_test_databridge_to_s3_lambda',
        schedule_interval=None,
        default_args=default_args,
        max_active_runs=1,
) as dag:

    databridge_to_s3_lambda = DataBridgeToS3LambdaOperator(
        table_name='li_imm_dang',
        table_schema='gis_lni',
    )

with DAG(
        dag_id='0_test_s3_to_databridge2_lambda',
        schedule_interval=None,
        default_args=default_args,
        max_active_runs=1,
) as dag:

    s3_to_databridge2_lambda = S3ToDataBridge2LambdaOperator(
        table_name='li_imm_dang',
        table_schema='lni',
    )