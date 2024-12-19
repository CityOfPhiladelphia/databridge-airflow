from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import GeopetlWriteOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta
from airflow.models import Variable


registered_local_businesses_schema = Variable.get('schemas') + 'registered_local_businesses.json'

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_db_carto_registered_local_businsses_v0', schedule_interval=None, default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from Databridge
extract_registered_local_businesses = GeopetlReadOperator(
    task_id='read_registered_local_businesses',
    csv_path='{{ ti.xcom_pull("make_staging") }}/registered_local_businesses.csv',
    dag=pipeline,
    db_conn_id='databridge',
    db_table_name='gis_procurement.registered_local_businesses',
    db_timestamp=False,
)

# ----------------------------------------------------
# Write files to Carto
write_registered_local_businesses = CartoUpdateOperator(
    task_id='write_registered_local_businesses_to_Carto',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/registered_local_businesses.csv',
    db_conn_id='carto_phl',
    db_table_name='registered_local_businesses',
    db_schema_json=registered_local_businesses_schema,
    db_select_users=['publicuser', 'tileuser']
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

extract_registered_local_businesses.set_upstream(make_staging)
extract_registered_local_businesses.set_downstream(write_registered_local_businesses)
write_registered_local_businesses.set_downstream(cleanup)
