from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.models import Variable

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_dev_opa_processed_deeds_v0', schedule_interval=None, default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract

extract_processed_deeds = GeopetlReadOperator(
    task_id='read_processed_deeds',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/processed_deeds.csv',
    db_conn_id='databridge2',
    db_table_name='cama.vw_processed_deeds',
    db_table_where='',
)


# ----------------------------------------------------
# Write

write_processed_deeds = GeopetlWriteOperator(
    task_id='write_processed_deeds',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/processed_deeds.csv',
    db_conn_id='databridge-cama',
    db_table_name='gis_cama.processed_deeds',
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}')

extract_processed_deeds.set_upstream(make_staging)
extract_processed_deeds.set_downstream(write_processed_deeds)
write_processed_deeds.set_downstream(cleanup)
