import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.hooks import GeopetlHook
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from datetime import datetime, timedelta
from airflow.models import Variable
import petl as etl

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 15, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('temp_etl_pin_owner_mailing_geopetl_v0', schedule_interval=None, default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from OPA

extract_pin_owner_mailing = GeopetlReadOperator(
    task_id='read_pin_owner_mailing',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pin_owner_mailing.csv',
    db_conn_id='databridge2',
    db_table_name='property.pin_owner_mailing',
    db_table_where='',
)

write_pin_owner_mailing = GeopetlWriteOperator(
    task_id='write_pin_owner_mailing',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pin_owner_mailing.csv',
    db_conn_id='databridge-gsg',
    db_table_name='gis_gsg.pin_owner_mailing',
    db_table_where='',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)


make_staging.set_downstream(extract_pin_owner_mailing)
extract_pin_owner_mailing.set_downstream(write_pin_owner_mailing)
write_pin_owner_mailing.set_downstream(cleanup)


