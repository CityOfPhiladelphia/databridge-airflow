from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta


sql = " select guid into '{}' from NG911_SiteAddresses "

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_ng911_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_ng911_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_ng911_siteaddresses_guids = MsSQLReadOperator(
    task_id='read_ng911_siteaddresses_guids',
    csv_path='{{ ti.xcom_pull("make_ng911_staging") }}/ng911_site_addresses_guids.csv',
    dag=pipeline,
    db_conn_id='ng911',
    db_table_name='NG911_SiteAddresses',
    db_fields='guid', 
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_ng911_siteaddresses_guids = GeopetlWriteOperator(
    task_id='write_ng911_siteaddresses_guids',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_ng911_staging") }}/ng911_site_addresses_guids.csv',
    db_conn_id='databridge2',
    db_table_name='ng911.ng911_siteaddresses',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_ng911_staging") }}',
)

extract_ng911_siteaddresses_guids.set_upstream(make_staging)
extract_ng911_siteaddresses_guids.set_downstream(write_ng911_siteaddresses_guids)
write_ng911_siteaddresses_guids.set_downstream(cleanup)

