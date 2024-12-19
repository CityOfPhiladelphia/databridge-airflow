from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta
from airflow.models import Variable


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    #'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
}

pipeline = DAG('etl_one_time_copy_rtt_summary_to_databridge_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

extract_rtt_summary = GeopetlReadOperator(
    task_id='read_rtt_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/rtt_summary.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_rtt_summary_for_db',
    db_table_where='',
)

write_rtt_summary = GeopetlWriteOperator(
    task_id='write_rtt_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/rtt_summary.csv',
    db_conn_id='databridge-dor',
    db_table_name='rtt_summary',
    db_table_where='',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

extract_rtt_summary.set_upstream(make_staging)
extract_rtt_summary.set_downstream(write_rtt_summary)
write_rtt_summary.set_downstream(cleanup)
