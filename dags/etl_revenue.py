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
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_revenue_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_revenue_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_ti_property = GeopetlReadOperator(
    task_id='read_ti_property',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue_staging") }}/ti_property.csv',
    db_conn_id='tips',
    db_table_name='ti_property',
    db_table_where='',
)

extract_ti_entity = GeopetlReadOperator(
    task_id='read_ti_entity',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue_staging") }}/ti_entity.csv',
    db_conn_id='tips',
    db_table_name='ti_entity',
    db_table_where='',
)

extract_ti_street_code = GeopetlReadOperator(
    task_id='read_ti_street_code',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue_staging") }}/ti_street_code.csv',
    db_conn_id='tips',
    db_table_name='ti_street_code',
    db_table_where='',
)
# ----------------------------------------------------
# Write extracted files to Databridge

write_ti_property = GeopetlWriteOperator(
    task_id='write_ti_property',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue_staging") }}/ti_property.csv',
    db_conn_id='databridge2',
    db_table_name='revenue.tips_ti_property',
)

write_ti_entity = GeopetlWriteOperator(
    task_id='write_ti_entity',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue_staging") }}/ti_entity.csv',
    db_conn_id='databridge2',
    db_table_name='revenue.tips_ti_entity',
)

write_ti_street_code = GeopetlWriteOperator(
    task_id='write_ti_street_code',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_revenue_staging") }}/ti_street_code.csv',
    db_conn_id='databridge2',
    db_table_name='revenue.tips_ti_street_code',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_revenue_staging") }}',
)

extract_ti_property.set_upstream(make_staging)
extract_ti_property.set_downstream(write_ti_property)
write_ti_property.set_downstream(cleanup)

extract_ti_entity.set_upstream(make_staging)
extract_ti_entity.set_downstream(write_ti_entity)
write_ti_entity.set_downstream(cleanup)

extract_ti_street_code.set_upstream(make_staging)
extract_ti_street_code.set_downstream(write_ti_street_code)
write_ti_street_code.set_downstream(cleanup)