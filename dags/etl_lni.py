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

pipeline = DAG('etl_lni_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_lni_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from LNI

extract_districts = GeopetlReadOperator(
    task_id='read_districts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/districts.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.districts',
    db_table_where='',
)

extract_permits = GeopetlReadOperator(
    task_id='read_permits',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/permits.csv',
    db_conn_id='hansen',
    db_table_name='gis_lni.li_permits',
    db_table_where='',
)

extract_violations = GeopetlReadOperator(
    task_id='read_violations',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/violations.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.li_violations',
    db_table_where='',
)

extract_eclipse_addressobjectid_mvw = GeopetlReadOperator(
    task_id='read_eclipse_addressobjectid_mvw',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/eclipse_addressobjectid_mvw.csv',
    db_conn_id='hansen',
    db_table_name='gis_lni.eclipse_addressobjectid_mvw',
    db_table_where='',
)

extract_eclipse_business_licenses = GeopetlReadOperator(
    task_id='read_eclipse_business_licenses',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/eclipse_business_licenses.csv',
    db_conn_id='hansen',
    db_table_name='gis_lni.eclipse_business_licenses',
    db_table_where='',
)

extract_parsed_addr = GeopetlReadOperator(
    task_id='read_parsed_addr',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/parsed_addr.csv',
    db_conn_id='hansen',
    db_table_name='gis_lni.parsed_addr',
    db_table_where='',
)

extract_li_building_footprints = GeopetlReadOperator(
    task_id='read_li_building_footprints_rev',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_building_footprints.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.li_building_footprints_rev',
    db_table_where='',
)


# ----------------------------------------------------
# Write extracted files to Databridge

write_districts = GeopetlWriteOperator(
    task_id='write_districts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/districts.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_districts',
)

write_permits = GeopetlWriteOperator(
    task_id='write_permits',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/permits.csv',
    db_conn_id='databridge2',
    db_table_name='lni.hansen_li_permits',
)

write_violations = GeopetlWriteOperator(
    task_id='write_violations',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/violations.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_li_violations',
)

write_eclipse_addressobjectid_mvw = GeopetlWriteOperator(
    task_id='write_ecplise_addressobjectid_mvw',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/eclipse_addressobjectid_mvw.csv',
    db_conn_id='databridge2',
    db_table_name='lni.hansen_eclipse_addressobjectid_mvw',
)

write_eclipse_business_licenses = GeopetlWriteOperator(
    task_id='write_eclipse_business_licenses',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/eclipse_business_licenses.csv',
    db_conn_id='databridge2',
    db_table_name='lni.hansen_eclipse_business_licenses',
)

write_parsed_addr = GeopetlWriteOperator(
    task_id='write_parsed_addr',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/parsed_addr.csv',
    db_conn_id='databridge2',
    db_table_name='lni.hansen_parsed_addr',
)

write_li_building_footprints = GeopetlWriteOperator(
    task_id='write_li_building_footprints',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_building_footprints.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_li_building_footprints',
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_lni_staging") }}',
)

extract_districts.set_upstream(make_staging)
extract_districts.set_downstream(write_districts)
write_districts.set_downstream(cleanup)

extract_permits.set_upstream(make_staging)
extract_permits.set_downstream(write_permits)
write_permits.set_downstream(cleanup)

extract_violations.set_upstream(make_staging)
extract_violations.set_downstream(write_violations)
write_violations.set_downstream(cleanup)

extract_eclipse_addressobjectid_mvw.set_upstream(make_staging)
extract_eclipse_addressobjectid_mvw.set_downstream(write_eclipse_addressobjectid_mvw)
write_eclipse_addressobjectid_mvw.set_downstream(cleanup)

extract_eclipse_business_licenses.set_upstream(make_staging)
extract_eclipse_business_licenses.set_downstream(write_eclipse_business_licenses)
write_eclipse_business_licenses.set_downstream(cleanup)

extract_parsed_addr.set_upstream(make_staging)
extract_parsed_addr.set_downstream(write_parsed_addr)
write_parsed_addr.set_downstream(cleanup)

extract_li_building_footprints.set_upstream(make_staging)
extract_li_building_footprints.set_downstream(write_li_building_footprints)
write_li_building_footprints.set_downstream(cleanup)

