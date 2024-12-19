from airflow import DAG
from airflow.contrib.operators.sftp_operator import SFTPOperator
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
}

pipeline = DAG('etl_test_sftp_operator_v0', schedule_interval=None, default_args=default_args)

test_file = '/home/ubuntu/databridge-airflow//SEPTA_STREET_ROUTES.csv'
sftp_file = 'CityGeo/test.csv'

test_write_file_to_sftp = SFTPOperator(
    task_id='test_sftp_operator',
    ssh_conn_id='sftp-main',
    dag=pipeline,
    local_filepath=test_file,
    remote_filepath=sftp_file,
    operation="put",
    create_intermediate_dirs=False,
)

