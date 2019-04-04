from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.models import Variable

# ============================================================
# Defaults - these arguments apply to all operators

ppr_hydration_stations_schema = Variable.get('schemas') + 'db_ppr_hydration_stations.json'

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

pipeline = DAG('etl_db_carto_ppr_hydration_stations_v0', schedule_interval='0 5 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_db_carto_ppr_hydration_stations_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from streets

extract_hydration_stations = GeopetlReadOperator(
    task_id='read_db_ppr_hydration_stations',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_carto_ppr_hydration_stations_staging") }}/db_ppr_hydration_stations.csv',
    db_conn_id='databridge',
    db_table_name='gis_ppr.ppr_hydrationstations',
    db_table_where='',
    db_timestamp=False,
)


# ----------------------------------------------------
# Write extracted files to Carto

write_hydration_stations = CartoUpdateOperator(
    task_id='write_db_ppr_hydration_stations',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_carto_ppr_hydration_stations_staging") }}/db_ppr_hydration_stations.csv',
    db_conn_id='carto_phl',
    db_table_name='ppr_hydration_stations',
    db_schema_json=ppr_hydration_stations_schema,
    db_select_users=['publicuser', 'tileuser']
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_db_carto_ppr_hydration_stations_staging") }}')

extract_hydration_stations.set_upstream(make_staging)
extract_hydration_stations.set_downstream(write_hydration_stations)
write_hydration_stations.set_downstream(cleanup)
