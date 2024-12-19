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


job_class_schema = Variable.get('schemas') + 'db_ohr_job_class.json'
pay_range_schema = Variable.get('schemas') + 'db_ohr_pay_range.json'
residency_waiver_schema = Variable.get('schemas') + 'db_ohr_residency_waiver.json'

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

pipeline = DAG('etl_db_carto_ohr_data_tables_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from Databridge

extract_job_class = GeopetlReadOperator(
    task_id='read_job_class',
    csv_path='{{ ti.xcom_pull("make_staging") }}/job_class.csv',
    dag=pipeline,
    db_conn_id='databridge',
    db_table_name='gis_ohr.job_class',
    db_timestamp=False,
)

extract_pay_range = GeopetlReadOperator(
    task_id='read_pay_range',
    csv_path='{{ ti.xcom_pull("make_staging") }}/pay_range.csv',
    dag=pipeline,
    db_conn_id='databridge',
    db_table_name='gis_ohr.pay_range',
    db_timestamp=False,
)

extract_residency_waiver = GeopetlReadOperator(
    task_id='read_residency_waiver',
    csv_path='{{ ti.xcom_pull("make_staging") }}/residency_waiver.csv',
    dag=pipeline,
    db_conn_id='databridge',
    db_table_name='gis_ohr.residency_waiver',
    db_timestamp=False,
)


# ----------------------------------------------------
# Write to Carto
write_job_class = CartoUpdateOperator(
    task_id='write_job_class',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/job_class.csv',
    db_conn_id='carto_phl',
    db_table_name='job_class',
    db_schema_json=job_class_schema,
    db_select_users=['publicuser', 'tileuser']
)

write_pay_range = CartoUpdateOperator(
    task_id='write_pay_range',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pay_range.csv',
    db_conn_id='carto_phl',
    db_table_name='pay_range',
    db_schema_json=pay_range_schema,
    db_select_users=['publicuser', 'tileuser']
)

write_residency_waiver = CartoUpdateOperator(
    task_id='write_residency_waiver',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/residency_waiver.csv',
    db_conn_id='carto_phl',
    db_table_name='residency_waiver',
    db_schema_json=residency_waiver_schema,
    db_select_users=['publicuser', 'tileuser']
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

extract_job_class.set_upstream(make_staging)
extract_job_class.set_downstream(write_job_class)
write_job_class.set_downstream(cleanup)

extract_pay_range.set_upstream(make_staging)
extract_pay_range.set_downstream(write_pay_range)
write_pay_range.set_downstream(cleanup)

extract_residency_waiver.set_upstream(make_staging)
extract_residency_waiver.set_downstream(write_residency_waiver)
write_residency_waiver.set_downstream(cleanup)

