from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.contrib.hooks.winrm_hook import WinRMHook
from airflow.contrib.operators.winrm_operator import WinRMOperator
from datetime import datetime, timedelta


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 4, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

with DAG(
    dag_id='test_winrm_v0', schedule_interval=None, default_args=default_args
) as pipeline:

    cmd = 'python c:/scripts/test_winrm.py'
    winRMHook = WinRMHook(ssh_conn_id='test_winrm')

    test = WinRMOperator(
        task_id="test_winrm",
        command=cmd,
        winrm_hook=winRMHook
    )

    test
