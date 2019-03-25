from airflow import DAG
from airflow.utils.history import update_history_table
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 4, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    'provide_context': True,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_history_test_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ----------------------------------------------------
# Update hash

update_history = PythonOperator(
    task_id='update_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'buildingcodes', 'hash_field': 'etl_hash'},
)


update_history
