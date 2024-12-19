import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from datetime import datetime, timedelta
from airflow.models import Variable
import petl as etl

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 15, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_cama_roll_geopetl_v2', schedule_interval=None, default_args=default_args)

# ------------------------------------------------------------
def delete_temp_file(**kwargs):
    path = kwargs['templates_dict']['csv_path']
    filename = kwargs['templates_dict']['filename']
    os.remove(path + '/' + filename)


# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from OPA

extract_asmt_roll = GeopetlReadOperator(
    task_id='read_asmt_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt_roll.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.asmt_roll',
    db_table_where='procid = 1014',
)

extract_pardat_roll = GeopetlReadOperator(
    task_id='read_pardat_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat_roll.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.pardat_roll',
    db_table_where='procid = 1014',
)

extract_owndat_roll = GeopetlReadOperator(
    task_id='read_owndat_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owndat_roll.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.owndat_roll',
    db_table_where='procid = 1014',
)

extract_proc_roll = GeopetlReadOperator(
    task_id='read_proc_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/proc_roll.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.proc_roll',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge
write_pardat_roll = GeopetlWriteOperator(
    task_id='write_pardat_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat_roll.csv',
    db_conn_id='databridge2',
    db_table_name='cama.pardat_roll',
    db_table_where='',
)

write_owndat_roll = GeopetlWriteOperator(
    task_id='write_owndat_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owndat_roll.csv',
    db_conn_id='databridge2',
    db_table_name='cama.owndat_roll',
    db_table_where='',
)

write_asmt_roll = GeopetlWriteOperator(
    task_id='write_asmt_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt_roll.csv',
    db_conn_id='databridge2',
    db_table_name='cama.asmt_roll',
    db_table_where='',
)

write_proc_roll = GeopetlWriteOperator(
    task_id='write_proc_roll',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/proc_roll.csv',
    db_conn_id='databridge2',
    db_table_name='cama.proc_roll',
    db_table_where='',
)

# -----------------------------------------------------------------
# Cleanup temp files
delete_temp_pardat_roll = PythonOperator(
    task_id='delete_temp_pardat_roll',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'pardat_roll.csv',},
)

delete_temp_owndat_roll = PythonOperator(
    task_id='delete_temp_owndat_roll',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'owndat_roll.csv',},
)

delete_temp_asmt_roll = PythonOperator(
    task_id='delete_temp_asmt_roll',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'asmt_roll.csv',},
)

delete_temp_proc_roll = PythonOperator(
    task_id='delete_temp_proc_roll',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'proc_roll.csv',},
)

# -----------------------------------------------------------------
# Update hashes
#
update_pardat_roll_hash = PythonOperator(
    task_id='update_pardat_roll_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'pardat_roll', 'hash_field': 'etl_hash'},
)

update_owndat_roll_hash = PythonOperator(
    task_id='update_owndat_roll_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'owndat_roll', 'hash_field': 'etl_hash'},
)

update_asmt_roll_hash = PythonOperator(
    task_id='update_asmt_roll_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'asmt_roll', 'hash_field': 'etl_hash'},
)

update_proc_roll_hash = PythonOperator(
    task_id='update_proc_roll_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'proc_roll', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories
#
update_pardat_roll_history = PythonOperator(
    task_id='update_pardat_roll_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'pardat_roll', 'hash_field': 'etl_hash'},
)

update_owndat_roll_history = PythonOperator(
    task_id='update_owndat_roll_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'owndat_roll', 'hash_field': 'etl_hash'},
)

update_asmt_roll_history = PythonOperator(
    task_id='update_asmt_roll_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'asmt_roll', 'hash_field': 'etl_hash'},
)

update_proc_roll_history = PythonOperator(
    task_id='update_proc_roll_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'proc_roll', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

#make_staging >> extract_splcom >> write_to_databridge >> cleanup
#
extract_pardat_roll.set_upstream(make_staging)
extract_owndat_roll.set_upstream(make_staging)
extract_asmt_roll.set_upstream(make_staging)
extract_proc_roll.set_upstream(make_staging)
extract_pardat_roll.set_downstream(write_pardat_roll)
extract_owndat_roll.set_downstream(write_owndat_roll)
extract_asmt_roll.set_downstream(write_asmt_roll)
extract_proc_roll.set_downstream(write_proc_roll)
write_pardat_roll.set_downstream(update_pardat_roll_hash)
write_pardat_roll.set_downstream(delete_temp_pardat_roll)
write_owndat_roll.set_downstream(update_owndat_roll_hash)
write_owndat_roll.set_downstream(delete_temp_owndat_roll)
write_asmt_roll.set_downstream(update_asmt_roll_hash)
write_asmt_roll.set_downstream(delete_temp_asmt_roll)
write_proc_roll.set_downstream(update_proc_roll_hash)
write_proc_roll.set_downstream(delete_temp_proc_roll)
update_pardat_roll_hash.set_downstream(update_pardat_roll_history)
update_owndat_roll_hash.set_downstream(update_owndat_roll_history)
update_asmt_roll_hash.set_downstream(update_asmt_roll_history)
update_proc_roll_hash.set_downstream(update_proc_roll_history)
update_pardat_roll_history.set_downstream(cleanup)
update_owndat_roll_history.set_downstream(cleanup)
update_asmt_roll_history.set_downstream(cleanup)
update_proc_roll_history.set_downstream(cleanup)
