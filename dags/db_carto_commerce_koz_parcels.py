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

commerce_koz_parcels_schema = Variable.get('schemas') + 'gis_commerce__koz_parcels.json'

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

pipeline = DAG('etl_db_carto_commerce_koz_parcels_v0', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_db_commerce_koz_parcels_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from streets

extract_koz_parcels = GeopetlReadOperator(
    task_id='read_db_commerce_koz_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_commerce_koz_parcels_staging") }}/db_commerce_koz_parcels.csv',
    db_conn_id='databridge',
    db_table_name='gis_commerce.koz_parcels',
    db_table_where='',
    db_timestamp=False,
)


# ----------------------------------------------------
# Write extracted files to Carto

write_koz_parcels = CartoUpdateOperator(
    task_id='write_db_commerce_koz_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_commerce_koz_parcels_staging") }}/db_commerce_koz_parcels.csv',
    db_conn_id='carto_gsg',
    db_table_name='koz_parcels',
    db_schema_json=commerce_koz_parcels_schema,
    db_select_users=['publicuser', 'tileuser']
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_db_commerce_koz_parcels_staging") }}')

extract_koz_parcels.set_upstream(make_staging)
extract_koz_parcels.set_downstream(write_koz_parcels)
write_koz_parcels.set_downstream(cleanup)
