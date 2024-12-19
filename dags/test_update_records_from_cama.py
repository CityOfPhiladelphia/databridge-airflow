from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import PetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.pin_sql import *
from airflow.utils.write_cama_exports_to_db import *
from datetime import datetime, timedelta
from airflow.models import Variable


TEST = True
test_suffix = '_test' if TEST else ''

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
}

pipeline = DAG('test_update_records_from_cama_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

def update_db(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)

# Update stage transaction status for pinned records:
update_stage_transaction_status_for_pinned_records =  PythonOperator(
    task_id='update_stage_transaction_status_for_pinned_records',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_stage_transaction_status_for_pinned_records_stmt.format(test_suffix=test_suffix)},
)

# Assign mapping queue and status:

set_research_queue_for_parcels_with_pin_tags =  PythonOperator(
    task_id='set_research_queue_for_parcels_with_pin_tags',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':research_queue_pin_tags_stmt.format(test_suffix=test_suffix)},
)

set_research_queue_for_non_one_to_one_matched_parcels =  PythonOperator(
    task_id='set_research_queue_for_one_one_to_one_matched_parcels',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':research_queue_non_one_to_one_parcels_stmt.format(test_suffix=test_suffix)},
)

set_mapping_queue_for_non_dt_parcels_not_in_research_queue =  PythonOperator(
    task_id='set_mapping_queue_for_non_dt_parcels_not_in_resaerch_queue',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':mapping_queue_non_dt_parcels_not_in_research_queue_stmt.format(test_suffix=test_suffix)},
)

set_mapping_queue_for_dt_parcels_with_pin_tags =  PythonOperator(
    task_id='set_mapping_queue_for_dt_parcels_with_pin_tags',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':mapping_queue_dt_parcels_with_pin_tags_stmt.format(test_suffix=test_suffix)},
)

update_status_to_pin_master_for_dt_with_no_tags = PythonOperator(
    task_id='update_status_to_pin_master_for_dt_with_no_tags',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_status_to_pin_master_for_dt_with_no_tags_stmt.format(test_suffix=test_suffix)},
)


update_stage_transaction_status_for_pinned_records.set_downstream(set_research_queue_for_parcels_with_pin_tags)
set_research_queue_for_parcels_with_pin_tags.set_downstream(set_research_queue_for_non_one_to_one_matched_parcels)
set_research_queue_for_non_one_to_one_matched_parcels.set_downstream(set_mapping_queue_for_non_dt_parcels_not_in_research_queue)
set_mapping_queue_for_non_dt_parcels_not_in_research_queue.set_downstream(set_mapping_queue_for_dt_parcels_with_pin_tags)
set_mapping_queue_for_dt_parcels_with_pin_tags.set_downstream(update_status_to_pin_master_for_dt_with_no_tags)
