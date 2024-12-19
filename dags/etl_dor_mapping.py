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
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_dor_mapping_geopetl_v0', schedule_interval='0 23 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_dor_mapping_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_doroem_parcel = GeopetlReadOperator(
    task_id='read_doroem_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_mapping_staging") }}/doroem_parcel.csv',
    db_conn_id='doroem',
    db_table_name='parcel',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_doroem_parcel = GeopetlWriteOperator(
    task_id='write_doroem_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_mapping_staging") }}/doroem_parcel.csv',
    db_conn_id='databridge2',
    db_table_name='dor.doroem_parcel',
    db_table_where='',
)

# -----------------------------------------------------------------
# Update hashes

update_doroem_parcel_hash = PythonOperator(
    task_id='update_doroem_parcel_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'doroem_parcel', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories

update_doroem_parcel_history = PythonOperator(
    task_id='update_doroem_parcel_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'doroem_parcel', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_dor_mapping_staging") }}',
)


extract_doroem_parcel.set_upstream(make_staging)
extract_doroem_parcel.set_downstream(write_doroem_parcel)
write_doroem_parcel.set_downstream(update_doroem_parcel_hash)
update_doroem_parcel_hash.set_downstream(update_doroem_parcel_history)
update_doroem_parcel_history.set_downstream(cleanup)
