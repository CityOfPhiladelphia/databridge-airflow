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


contractor_violations_schema = Variable.get('schemas') + 'db_lni_contractor_violations.json'

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

pipeline = DAG('etl_db_carto_lni_contractor_violations_v0', schedule_interval='10 8 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from Databridge

extract_contractor_violations = GeopetlReadOperator(
    task_id='read_contractor_violations',
    csv_path='{{ ti.xcom_pull("make_staging") }}/contractor_violations.csv',
    dag=pipeline,
    db_conn_id='databridge',
    db_table_name='gis_lni.contractor_violations',
    db_timestamp=False,
)

# ----------------------------------------------------
# Write to Carto
write_contractor_violations = CartoUpdateOperator(
    task_id='write_contrator_violations',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/contractor_violations.csv',
    db_conn_id='carto_phl',
    db_table_name='contractor_violations',
    db_schema_json=contractor_violations_schema,
    db_select_users=['publicuser', 'tileuser']
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

extract_contractor_violations.set_upstream(make_staging)
extract_contractor_violations.set_downstream(write_contractor_violations)
write_contractor_violations.set_downstream(cleanup)
