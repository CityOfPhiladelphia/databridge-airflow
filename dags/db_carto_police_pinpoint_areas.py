from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import GeopetlReadOperator
from carto_truncate_append_plugin import CartoTruncateAppendOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta
from airflow.models import Variable


pinpoint_areas_schema = Variable.get('schemas') + 'db_police_pinpoint_areas.json'

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

pipeline = DAG('etl_db_carto_police_pinpoint_areas_v0', schedule_interval='10 10 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from Databridge

extract_pinpoint_areas = GeopetlReadOperator(
    task_id='read_pinpoint_areas',
    csv_path='{{ ti.xcom_pull("make_staging") }}/pinpoint_areas.csv',
    dag=pipeline,
    db_conn_id='databridge',
    db_table_name='gis_police.pinpoint_areas',
    db_timestamp=False,
)

# ----------------------------------------------------
# Write to Carto
truncate_append_pinpoint_areas = CartoTruncateAppendOperator(
    task_id='truncate_append_pinpoint_areas',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pinpoint_areas.csv',
    db_conn_id='carto_phl',
    db_table_name='pinpoint_areas',
    db_schema_json=pinpoint_areas_schema,
#    db_select_users=['publicuser', 'tileuser']
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

extract_pinpoint_areas.set_upstream(make_staging)
extract_pinpoint_areas.set_downstream(truncate_append_pinpoint_areas)
truncate_append_pinpoint_areas.set_downstream(cleanup)
