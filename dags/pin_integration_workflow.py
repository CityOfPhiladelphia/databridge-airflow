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
extract_new_deeds_table_name = 'property.vw_new_records_for_cama'
write_new_deeds_table_name = 'PROPERTY_DEEDS_NEW'
if TEST:
    extract_new_deeds_table_name = extract_new_deeds_table_name + test_suffix
    write_new_deeds_table_name = write_new_deeds_table_name + test_suffix.upper()

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

pipeline = DAG('pin_cama_integration_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_pin_cama_integration_staging',
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
# Write extracted files to Databridge

#write_beverage_tax_registration_data = GeopetlWriteOperator(
#    task_id='write_eGov3_TaxpayerRegistration_V',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/eGov3_TaxpayerRegistration_V.csv',
#    db_conn_id='databridge2',
#    db_table_name='revenue.egov3_taxpayerregistration_v',
#)

# Insert transformed DOR records into staging tables:
def update_db(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)

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
    csv_path='{{ ti.xcom_pull("make_pin_cama_integration_staging") }}/vw_new_records_for_cama.csv',
    db_conn_id='databridge2',
    db_table_name=extract_new_deeds_table_name,
    db_table_where='',
)

write_new_deeds_for_cama = PetlWriteOperator(
    task_id='write_new_deeds_for_cama',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_pin_cama_integration_staging") }}/vw_new_records_for_cama.csv',
    db_conn_id='databridge-cama',
    db_table_name=write_new_deeds_table_name,
    db_table_where='',
)

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


# Update status:



# ----------------------------------------------------
# Read transformed data from Databridge

#read_transformed_beverage_tax_registration_data = GeopetlReadOperator(
#    task_id='read_transformed_beverage_tax_registration_data',
#    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/vw_beverage_tax_registration_data.csv',
#    dag=pipeline,
#    db_conn_id='databridge2',
#    db_table_name='revenue.vw_beverage_tax_registration_data',
#)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_pin_cama_integration_staging") }}',
)

insert_records_into_stage_parcel.set_upstream(make_staging)
insert_records_into_stage_transaction.set_upstream(make_staging)

extract_new_deeds_for_cama.set_upstream(insert_records_into_stage_parcel)
extract_new_deeds_for_cama.set_upstream(insert_records_into_stage_transaction)

write_new_deeds_for_cama.set_upstream(extract_new_deeds_for_cama)
write_new_deeds_for_cama.set_downstream(update_stage_transaction_status_for_pinned_records)

update_stage_transaction_status_for_pinned_records.set_downstream(set_research_queue_for_parcels_with_pin_tags)
set_research_queue_for_parcels_with_pin_tags.set_downstream(set_research_queue_for_non_one_to_one_matched_parcels)
set_research_queue_for_non_one_to_one_matched_parcels.set_downstream(set_mapping_queue_for_non_dt_parcels_not_in_research_queue)
set_mapping_queue_for_non_dt_parcels_not_in_research_queue.set_downstream(set_mapping_queue_for_dt_parcels_with_pin_tags)
set_mapping_queue_for_dt_parcels_with_pin_tags.set_downstream(update_status_to_pin_master_for_dt_with_no_tags)
update_status_to_pin_master_for_dt_with_no_tags.set_downstream(cleanup)
