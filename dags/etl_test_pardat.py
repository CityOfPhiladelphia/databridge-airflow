from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from datetime import datetime, timedelta


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 15, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_cama_test_pardat_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from OPA

extract_pardat = GeopetlReadOperator(
    task_id='read_pardat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.pardat',
    db_table_where='',
)
# ----------------------------------------------------
# Write extracted files to Databridge

write_pardat = GeopetlWriteOperator(
    task_id='write_pardat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat.csv',
    db_conn_id='databridge2',
    db_table_name='cama.pardat',
    db_table_where='',
)
# -----------------------------------------------------------------
# Update hashes
update_pardat_hash = PythonOperator(
    task_id='update_pardat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'pardat', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories

update_pardat_history = PythonOperator(
    task_id='update_pardat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'pardat', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

#make_staging >> extract_splcom >> write_to_databridge >> cleanup

extract_pardat.set_upstream(make_staging)
extract_pardat.set_downstream(write_pardat)
write_pardat.set_downstream(update_pardat_hash)
update_pardat_hash.set_downstream(update_pardat_history)
update_pardat_history.set_downstream(cleanup)
