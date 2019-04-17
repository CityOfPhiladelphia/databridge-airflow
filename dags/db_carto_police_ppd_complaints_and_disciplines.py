from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.models import Variable

# ============================================================
# Defaults - these arguments apply to all operators

police_ppd_complaints_schema = Variable.get('schemas') + 'gis_police__ppd_complaints.json'
police_ppd_complaint_disciplines_schema = Variable.get('schemas') + 'gis_police__ppd_complaint_disciplines.json'


default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_db_carto_police_ppd_complaints_and_disciplines_v0', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_police_ppd_complaints_and_disciplines_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from db2 police account:

extract_ppd_complaints = GeopetlReadOperator(
    task_id='read_ppd_complaints',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_police_ppd_complaints_and_disciplines_staging") }}/police_ppd_complaints.csv',
    db_conn_id='databridge2',
    db_table_name='police.ppd_complaints',
    db_table_where='',
    db_timestamp=False,
)


extract_ppd_disciplines = GeopetlReadOperator(
    task_id='read_ppd_disciplines',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_police_ppd_complaints_and_disciplines_staging") }}/police_ppd_complaint_disciplines.csv',
    db_conn_id='databridge2',
    db_table_name='police.ppd_complaint_disciplines',
    db_table_where='',
    db_timestamp=False,
)

# ----------------------------------------------------
# Write extracted files to Carto

write_ppd_complaints = CartoUpdateOperator(
    task_id='write_police_ppd_complaints',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_police_ppd_complaints_and_disciplines_staging") }}/police_ppd_complaints.csv',
    db_conn_id='carto_phl',
    db_table_name='ppd_complaints',
    db_schema_json=police_ppd_complaints_schema,
    db_select_users=['publicuser', 'tileuser']
)

write_ppd_disciplines = CartoUpdateOperator(
    task_id='write_police_ppd_disciplines',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_police_ppd_complaints_and_disciplines_staging") }}/police_ppd_complaint_disciplines.csv',
    db_conn_id='carto_phl',
    db_table_name='ppd_complaint_disciplines',
    db_schema_json=police_ppd_complaint_disciplines_schema,
    db_select_users=['publicuser', 'tileuser']
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_police_ppd_complaints_and_disciplines_staging") }}')

extract_ppd_complaints.set_upstream(make_staging)
extract_ppd_disciplines.set_upstream(make_staging)
extract_ppd_complaints.set_downstream(write_ppd_complaints)
extract_ppd_disciplines.set_downstream(write_ppd_disciplines)

write_ppd_complaints.set_downstream(cleanup)
write_ppd_disciplines.set_downstream(cleanup)
