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

pipeline = DAG('etl_ais_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_ais_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from AIS

extract_address_servicearea_summary = GeopetlReadOperator(
    task_id='read_address_servicearea_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_ais_staging") }}/address_servicearea_summary.csv',
    db_conn_id='databridge',
    db_table_name='gis_ais.vw_address_servicearea_summary',
    db_table_where='',
)

extract_dor_parcel_address_check = GeopetlReadOperator(
    task_id='read_dor_parcel_address_check',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_ais_staging") }}/dor_parcel_address_check.csv',
    db_conn_id='databridge',
    db_table_name='gis_ais.dor_parcel_address_check',
    db_table_where='',
)


# ----------------------------------------------------
# Write extracted files to Databridge

write_address_servicearea_summary = GeopetlWriteOperator(
    task_id='write_address_servicearea_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_ais_staging") }}/address_servicearea_summary.csv',
    db_conn_id='databridge2',
    db_table_name='ais.databridge_address_servicearea_summary',
)

write_dor_parcel_address_check = GeopetlWriteOperator(
    task_id='write_dor_parcel_address_check',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_ais_staging") }}/dor_parcel_address_check.csv',
    db_conn_id='databridge2',
    db_table_name='ais.databridge_dor_parcel_address_check',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_ais_staging") }}',
)

extract_address_servicearea_summary.set_upstream(make_staging)
extract_address_servicearea_summary.set_downstream(write_address_servicearea_summary)
write_address_servicearea_summary.set_downstream(cleanup)

extract_dor_parcel_address_check.set_upstream(make_staging)
extract_dor_parcel_address_check.set_downstream(write_dor_parcel_address_check)
write_dor_parcel_address_check.set_downstream(cleanup)

