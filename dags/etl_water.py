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

pipeline = DAG('etl_water_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_water_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from LNI

extract_pwd_parcels = GeopetlReadOperator(
    task_id='read_pwd_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_water_staging") }}/pwd_parcels.csv',
    db_conn_id='databridge',
    db_table_name='gis_water.pwd_parcels',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_pwd_parcels = GeopetlWriteOperator(
    task_id='write_pwd_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_water_staging") }}/pwd_parcels.csv',
    db_conn_id='databridge2',
    db_table_name='water.databridge_pwd_parcels',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_water_staging") }}',
)

extract_pwd_parcels.set_upstream(make_staging)
extract_pwd_parcels.set_downstream(write_pwd_parcels)
write_pwd_parcels.set_downstream(cleanup)
