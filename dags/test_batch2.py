from airflow import DAG
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook
# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('test_batch2', schedule_interval='0 5 * * *', default_args=default_args)

connection = BaseHook.get_connection("databridge")

print(connection.host, connection.conn_type, connection.conn_id, connection.extra, connection.port, connection.login)

batch_test = AWSBatchOperator(
    job_name='airflow-batch-test',
    job_definition='test_extract_and_load_to_databridge',
    job_queue='databridge-airflow',
    region_name='us-east-1',
    overrides={'command': ["python3 /extract_and_load_to_databridge.py", "extract", "db_type={}".format(connection.conn_type), "db_host={}".format(connection.host), "db_user={}".format(connection.login), "db_password={}".format(connection.password), "db_name={}".format(connection.extra), "db_port={}".format(connection.port), "db_table_schema=gis_lni", "db_table_name=li_imm_dang", "s3_bucket=citygeo-airflow-databridge2"]},
    task_id='test_extract',
    dag=pipeline
)

batch_test
