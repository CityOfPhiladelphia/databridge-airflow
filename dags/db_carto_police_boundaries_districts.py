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

police_boundaries_district_schema = Variable.get('schemas') + 'db_police_boundaries_district.json'

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_db_carto_police_boundaries_district_v0', schedule_interval=None, default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_db_carto_police_boundaries_district_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from streets

extract_police_boundaries_district = GeopetlReadOperator(
    task_id='read_db_police_boundaries_district',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_carto_police_boundaries_district_staging") }}/db_police_boundaries_district.csv',
    db_conn_id='databridge',
    db_table_name='gis_police.boundaries_district',
    db_table_where='',
    db_timestamp=False,
)


# ----------------------------------------------------
# Write extracted files to Carto

write_police_boundaries_district = CartoUpdateOperator(
    task_id='write_db_police_boundaries_district',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_carto_police_boundaries_district_staging") }}/db_police_boundaries_district.csv',
    db_conn_id='carto_phl',
    db_table_name='police_districts',
    db_schema_json=police_boundaries_district_schema,
    db_select_users=['publicuser', 'tileuser']
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_db_carto_police_boundaries_district_staging") }}')

extract_police_boundaries_district.set_upstream(make_staging)
extract_police_boundaries_district.set_downstream(write_police_boundaries_district)
write_police_boundaries_district.set_downstream(cleanup)
