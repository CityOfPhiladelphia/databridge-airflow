import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.hooks import GeopetlHook
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.pin_source_address_std import check_address_comps
from airflow.utils.pin_sql_v2 import *
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

pipeline = DAG('etl_cama_exdet_v0', schedule_interval=None, default_args=default_args)

# -----------------------------------------------------------
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

extract_exdet = GeopetlReadOperator(
    task_id='read_exdet',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/exdet.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.exdet',
    db_sql='select parid, taxyr, excode, begdt, wen, current_timestamp as etl_read_timestamp from philly_prod.exdet',
)

write_exdet = GeopetlWriteOperator(
    task_id='write_exdet',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/exdet.csv',
    db_conn_id='databridge2',
    db_table_name='cama.exdet',
    db_table_where='',
)

delete_temp_exdet = PythonOperator(
    task_id='delete_temp_exdet',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'exdet.csv',},
)

update_exdet_hash = PythonOperator(
    task_id='update_exdet_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'exdet', 'hash_field': 'etl_hash'},
)

update_exdet_history = PythonOperator(
    task_id='update_exdet_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'exdet', 'hash_field': 'etl_hash'},
)

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

extract_exdet.set_upstream(make_staging)
extract_exdet.set_downstream(write_exdet)
write_exdet.set_downstream(update_exdet_hash)
write_exdet.set_downstream(delete_temp_exdet)
update_exdet_hash.set_downstream(update_exdet_history)
update_exdet_history.set_downstream(cleanup)

