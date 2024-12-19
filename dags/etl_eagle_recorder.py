from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta
from airflow.models import Variable


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

pipeline = DAG('etl_eagle_recorder_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_eagle_recorder_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_document_data = MsSQLReadOperator(
    task_id='read_document_data',
    csv_path='{{ ti.xcom_pull("make_eagle_recorder_staging") }}/document_data.csv',
    dag=pipeline,
    db_conn_id='eagle_recorder',
    db_table_name='DocumentData',
    db_fields='DocumentID DocumentData',
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_document_data = GeopetlWriteOperator(
    task_id='write_document_data',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_eagle_recorder_staging") }}/document_data.csv',
    db_conn_id='databridge2',
    db_table_name='dor.eagle_recorder_document_data',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_eagle_recorder_staging") }}',
)

extract_document_data.set_upstream(make_staging)
extract_document_data.set_downstream(write_document_data)
write_document_data.set_downstream(cleanup)

