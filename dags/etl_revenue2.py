from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
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

pipeline = DAG('etl_revenue2_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_revenue2_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from REVENUE

extract_ti_name = GeopetlReadOperator(
    task_id='read_ti_name',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue2_staging") }}/ti_name.csv',
    db_conn_id='tips',
    db_table_name='ti_name',
    db_table_where='',
)

extract_f009 = GeopetlReadOperator(
    task_id='read_f009',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue2_staging") }}/f009.csv',
    db_conn_id='tips',
    db_table_name='f009',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_ti_name = GeopetlWriteOperator(
    task_id='write_ti_name',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue2_staging") }}/ti_name.csv',
    db_conn_id='databridge2',
    db_table_name='revenue.tips_ti_name',
)

write_f009 = GeopetlWriteOperator(
    task_id='write_f009',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue2_staging") }}/f009.csv',
    db_conn_id='databridge2',
    db_table_name='revenue.tips_f009',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_revenue2_staging") }}',
)


extract_ti_name.set_upstream(make_staging)
extract_ti_name.set_downstream(write_ti_name)
write_ti_name.set_downstream(cleanup)

extract_f009.set_upstream(make_staging)
extract_f009.set_downstream(write_f009)
write_f009.set_downstream(cleanup)
