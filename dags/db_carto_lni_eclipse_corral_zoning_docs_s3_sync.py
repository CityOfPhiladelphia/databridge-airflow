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

eclipse_corral_zoning_docs_s3_sync_schema = Variable.get('schemas') + 'eclipse__corral_zoning_docs_s3_sync.json'

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_db_carto_li_eclipse_corral_zoning_docs_s3_sync_v1', schedule_interval='0 6 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_db2_eclipse_corral_zoning_docs_s3_sync_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from streets

extract_eclipse_corral_zoning_docs_s3_sync = GeopetlReadOperator(
    task_id='read_db2_eclipse_corral_zoning_docs_s3_sync',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db2_eclipse_corral_zoning_docs_s3_sync_staging") }}/db2_eclipse_corral_zoning_docs_s3_sync.csv',
    db_conn_id='databridge2',
    db_table_name='eclipse.vw_li_zoning_docs',
    db_table_where='',
    db_timestamp=False,
)


# ----------------------------------------------------
# Write extracted files to Carto

write_eclipse_corral_zoning_docs_s3_sync = CartoUpdateOperator(
    task_id='write_db2_eclipse_corral_zoning_docs_s3_sync',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db2_eclipse_corral_zoning_docs_s3_sync_staging") }}/db2_eclipse_corral_zoning_docs_s3_sync.csv',
    db_conn_id='carto_phl',
    db_table_name='li_zoning_docs',
    db_schema_json=eclipse_corral_zoning_docs_s3_sync_schema,
    db_select_users=['publicuser', 'tileuser']
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_db2_eclipse_corral_zoning_docs_s3_sync_staging") }}')

extract_eclipse_corral_zoning_docs_s3_sync.set_upstream(make_staging)
extract_eclipse_corral_zoning_docs_s3_sync.set_downstream(write_eclipse_corral_zoning_docs_s3_sync)
write_eclipse_corral_zoning_docs_s3_sync.set_downstream(cleanup)

