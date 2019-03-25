from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
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

pipeline = DAG('etl_streets_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_streets_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from LNI

extract_street_centerline = GeopetlReadOperator(
    task_id='read_street_centerline',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_streets_staging") }}/street_centerline.csv',
    db_conn_id='databridge',
    db_table_name='gis_streets.street_centerline',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_street_centerline = GeopetlWriteOperator(
    task_id='write_street_centerline',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_streets_staging") }}/street_centerline.csv',
    db_conn_id='databridge2',
    db_table_name='streets.databridge_street_centerline',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_streets_staging") }}',
)

extract_street_centerline.set_upstream(make_staging)
extract_street_centerline.set_downstream(write_street_centerline)
write_street_centerline.set_downstream(cleanup)
