import petl as etl
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
extract_new_deeds_table_name = 'property.vw_new_records_for_cama{}'.format(test_suffix)
write_new_deeds_table_name = 'PROPERTY_DEEDS_NEW'
write_new_deeds_old_table_name = 'PROPERTY_DEEDS'

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

pipeline = DAG('test_records_to_cama_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_test_records_to_cama_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read new records from DOR (tyler eagle db or API)
#   *** not implemented; currently test files are loaded as extract tables in DOR account in db ***

#extract_new_records = MsSQLReadOperator(
#    task_id='read_eGov3_TaxpayerRegistration_V',
#    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/eGov3_TaxpayerRegistration_V.csv',
#    dag=pipeline,
#    db_conn_id='bevtax',
#    db_table_name='eGov3_TaxpayerRegistration_V',
#    db_fields='RegistrationType, OrganisationName, DBAName, BusinessAddress, BusinessUnitApt, BusinessCity, BusinessState, BusinessZipCode, BusinessPhone, BusinessPhExt, BusinessWebSite, StartDate, EndDate', 
#)

# ----------------------------------------------------

def format_records_for_cama_old_table(**kwargs):
    csv_path = kwargs['templates_dict']['csv_path']
    infile_name = kwargs['templates_dict']['infile_name']
    outfile_suffix = kwargs['templates_dict']['outfile_suffix']
    infile = csv_path + '/' + infile_name + '.csv'
    outfile = csv_path + '/' + infile_name + outfile_suffix + '.csv'
    etl.fromcsv(infile).cutout('opa_queue').addfield('ordinance_indicator').tocsv(outfile)

def update_db(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)


refresh_pin_master = PythonOperator(
    task_id='refresh_pin_master',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':refresh_pin_master_stmt.format(test_suffix=test_suffix)},
)

insert_records_into_stage_parcel = PythonOperator(
    task_id='insert_records_into_stage_parcel',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':stage_parcel_insert_dor_stmt.format(test_suffix=test_suffix)},
)

insert_records_into_stage_transaction = PythonOperator(
    task_id='insert_records_into_stage_transaction',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':stage_transaction_insert_dor_stmt.format(test_suffix=test_suffix)},
)

# Export for CAMA:
extract_new_deeds_for_cama = GeopetlReadOperator(
    task_id='read_property_vw_new_records_for_cama',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_test_records_to_cama_staging") }}/vw_new_records_for_cama.csv',
    db_conn_id='databridge2',
    db_table_name=extract_new_deeds_table_name,
    db_table_where='',
)

write_new_deeds_for_cama = PetlWriteOperator(
    task_id='write_new_deeds_for_cama',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_test_records_to_cama_staging") }}/vw_new_records_for_cama.csv',
    db_conn_id='databridge-cama',
    db_table_name=write_new_deeds_table_name,
    append=True,
    db_table_where='',
)

format_new_deeds_for_old_table = PythonOperator(
    task_id='format_new_deeds_for_old_table',
    dag=pipeline,
    python_callable=format_records_for_cama_old_table,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_test_records_to_cama_staging") }}','infile_name': 'vw_new_records_for_cama', 'outfile_suffix':'_fmt'},
)

write_new_deeds_for_cama_old_table = PetlWriteOperator(
    task_id='write_new_deeds_for_cama_old_table',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_test_records_to_cama_staging") }}/vw_new_records_for_cama_fmt.csv',
    db_conn_id='databridge-cama',
    db_table_name=write_new_deeds_old_table_name,
    append=True,
    db_table_where='',
)

# Update stage transaction status for records sent to opa queue:
update_stage_transaction_status_for_opa_queued_records =  PythonOperator(
    task_id='update_stage_transaction_status_for_opa_queued_records',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_stage_transaction_status_for_opa_queued_records_stmt.format(test_suffix=test_suffix)},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_test_records_to_cama_staging") }}',
)

refresh_pin_master.set_upstream(make_staging)

insert_records_into_stage_parcel.set_upstream(refresh_pin_master)
insert_records_into_stage_transaction.set_upstream(refresh_pin_master)

extract_new_deeds_for_cama.set_upstream(insert_records_into_stage_parcel)
extract_new_deeds_for_cama.set_upstream(insert_records_into_stage_transaction)

extract_new_deeds_for_cama.set_downstream(format_new_deeds_for_old_table)
write_new_deeds_for_cama.set_upstream(extract_new_deeds_for_cama)

write_new_deeds_for_cama_old_table.set_upstream(format_new_deeds_for_old_table)

write_new_deeds_for_cama.set_downstream(update_stage_transaction_status_for_opa_queued_records)
write_new_deeds_for_cama_old_table.set_downstream(update_stage_transaction_status_for_opa_queued_records)

update_stage_transaction_status_for_opa_queued_records.set_downstream(cleanup)
