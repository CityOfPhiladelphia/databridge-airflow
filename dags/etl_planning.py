from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert, slack_success_alert


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

pipeline = DAG('etl_planning_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_planning_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from LNI

extract_zoning_basedistricts = GeopetlReadOperator(
    task_id='read_zoning_basedistricts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_planning_staging") }}/zoning_basedistricts.csv',
    db_conn_id='databridge',
    db_table_name='gis_planning.zoning_basedistricts',
    db_table_where='',
)


# ----------------------------------------------------
# Write extracted files to Databridge

write_zoning_basedistricts = GeopetlWriteOperator(
    task_id='write_zoning_basedistricts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_planning_staging") }}/zoning_basedistricts.csv',
    db_conn_id='databridge2',
    db_table_name='planning.databridge_zoning_basedistricts',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_planning_staging") }}'
)


extract_zoning_basedistricts.set_upstream(make_staging)
extract_zoning_basedistricts.set_downstream(write_zoning_basedistricts)
write_zoning_basedistricts.set_downstream(cleanup)

