from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator, GeopetlUpsertOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.models import Variable
from airflow.utils.pin_sql_v2 import *
from datetime import datetime, timedelta
import petl as etl

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

pipeline = DAG('etl_lni_geopetl_v0', schedule_interval='0 11 * * *', default_args=default_args)

############
#
# UTILS:
#
############
def update_db(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)

def extract_from_postgres(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    stmt_where=kwargs.get('stmt_where', '')
    table_name=kwargs['table_name']
    update_date_file = templates_dict.get('update_date_csv_path', '')
    if update_date_file:
        last_update_date = etl.fromcsv(update_date_file)[1][0]
        if not last_update_date:
        # assign default old update date so doesn't act as filter:
            last_update_date = '1801-01-01'
        if stmt_where:
            stmt_where=stmt_where.format(last_update_date=last_update_date)
            stmt = stmt + ' WHERE ' + stmt_where if last_update_date else stmt
        else:
            stmt = stmt.format(last_update_date=last_update_date)
    print(stmt)
    
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    #pg_hook.bulk_dump(table_name, csv_path)
    pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)

# get last updated timestamp from db oracle table:
get_last_update_from_oracle_stmt = '''select max(etl_modified_timestamp) as last_update from {table_name}'''

# get updates since last updated timestamp from db2:
get_updates_from_db2_stmt = '''
    select {fields}
    from {table_name}
'''
get_updates_from_db2_stmt_where = ''' etl_modified_timestamp > '{last_update_date}' '''
################

db2_inspections_for_cama_table_name = 'lni.vw_inspections_for_cama_w_pin_from_history_v3'
inspections_update_fields = '''pin, license_number, business_mailing_address, business_name, full_address, 
inactive_date,initial_issue_date, legal_entity_type, legal_first_name, legal_last_name, legal_name, license_status, license_type, 
most_recent_issue_date, number_of_units, opa_account_number, revenue_code, street_address, zip_code, etl_modified_timestamp'''

db2_permits_for_cama_table_name = 'lni.vw_permits_for_cama_w_pin_from_history_v2'
permits_update_fields = '''pin,permit_number,permit_issue_date,declared_value,opa_account_number,address_concat,address_base,unit_number,
address_key,zip_code,permit_type,permit_description,permit_location,type_of_work,area_of_work,owner_name,owner_address_1,owner_address_2,
owner_city,owner_state,owner_zip_code,owner_email,owner_phone,contractor_name,contractor_address_1,contractor_address_2,contractor_city,
contractor_state,contractor_zip_code,number_of_plans,approved_use,occupancy_class,final_inspection_date,permit_status,description_of_work_part1,
description_of_work_part2,description_of_work_part3,etl_modified_timestamp '''

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_lni_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from LNI

extract_permits = GeopetlReadOperator(
    task_id='read_permits',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/permits.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.permits',
    db_table_where='',
)

extract_business_licenses = GeopetlReadOperator(
    task_id='read_business_licenses',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/business_licenses.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.business_licenses',
    db_table_where='',
)

extract_li_districts = GeopetlReadOperator(
    task_id='read_li_districts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_districts.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.li_districts',
    db_table_where='',
)

extract_violations = GeopetlReadOperator(
    task_id='read_violations',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/violations.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.violations',
    db_table_where='',
)

#extract_eclipse_addressobjectid_mvw = GeopetlReadOperator(
#    task_id='read_eclipse_addressobjectid_mvw',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/eclipse_addressobjectid_mvw.csv',
#    db_conn_id='hansen',
#    db_table_name='gis_lni.eclipse_addressobjectid_mvw',
#    db_table_where='',
#)

# extract_parsed_addr = GeopetlReadOperator(
#     task_id='read_parsed_addr',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_lni_staging") }}/parsed_addr.csv',
#     db_conn_id='hansen',
#     db_table_name='gis_lni.parsed_addr',
#     db_table_where='',
# )

extract_li_building_footprints = GeopetlReadOperator(
    task_id='read_li_building_footprints',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_building_footprints.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.li_building_footprints',
    db_table_where='',
)

#extract_bp_pp_ep_issuedfinalinsp = GeopetlReadOperator(
#    task_id='read_bp_pp_ep_issuedfinalinsp',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_bp_pp_ep_issuedfinalinsp.csv',
#    db_conn_id='databridge',
#    db_table_name='gis_lni.li_bp_pp_ep_issuedfinalinsp',
#    db_table_where='',
#)
# 
# ----------------------------------------------------
# Write extracted files to Databridge
write_permits = GeopetlWriteOperator(
    task_id='write_permits',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/permits.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_permits',
)

write_business_licenses = GeopetlWriteOperator(
    task_id='write_business_licenses',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/business_licenses.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_business_licenses',
)

write_li_districts = GeopetlWriteOperator(
    task_id='write_li_districts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_districts.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_li_districts',
)

write_violations = GeopetlWriteOperator(
    task_id='write_violations',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/violations.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_violations',
)

#write_eclipse_addressobjectid_mvw = GeopetlWriteOperator(
#    task_id='write_ecplise_addressobjectid_mvw',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/eclipse_addressobjectid_mvw.csv',
#    db_conn_id='databridge2',
#    db_table_name='lni.hansen_eclipse_addressobjectid_mvw',
#)


# write_parsed_addr = GeopetlWriteOperator(
#     task_id='write_parsed_addr',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_lni_staging") }}/parsed_addr.csv',
#     db_conn_id='databridge2',
#     db_table_name='lni.hansen_parsed_addr',
# )

write_li_building_footprints = GeopetlWriteOperator(
    task_id='write_li_building_footprints',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_building_footprints.csv',
    db_conn_id='databridge2',
    db_table_name='lni.databridge_li_building_footprints',
)

#write_bp_pp_ep_issuedfinalinsp =  GeopetlWriteOperator(
#    task_id='write_bp_pp_ep_issuedfinalinsp',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/li_bp_pp_ep_issuedfinalinsp.csv',
#    db_conn_id='databridge2',
#    db_table_name='lni.databridge_bp_pp_ep_issuedfinalinsp',
#)

# -----------------------------------------------------------------
# Update hashes

update_permits_hash = PythonOperator(
    task_id='update_permits_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'databridge_permits', 'hash_field': 'etl_hash'},
)

update_business_licenses_hash = PythonOperator(
    task_id='update_business_licenses_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'databridge_business_licenses', 'hash_field': 'etl_hash'},
)

#update_bp_pp_ep_issuedfinalinsp_hash = PythonOperator(
#    task_id='update_bp_pp_ep_issuedfinalinsp_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'databridge_bp_pp_ep_issuedfinalinsp', 'hash_field': 'etl_hash'},
#)

# -----------------------------------------------------------------
# Update histories

update_permits_history = PythonOperator(
    task_id='update_permits_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'databridge_permits', 'hash_field': 'etl_hash'},
)

update_business_licenses_history = PythonOperator(
    task_id='update_business_licenses_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'databridge_business_licenses', 'hash_field': 'etl_hash'},
)

#update_bp_pp_ep_issuedfinalinsp_history = PythonOperator(
#    task_id='update_bp_pp_ep_issuedfinalinsp_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'databridge_bp_pp_ep_issuedfinalinsp', 'hash_field': 'etl_hash'},
#)

# Updage permits and inspections for CAMA

extract_permits_for_cama =  GeopetlReadOperator(
   task_id='read_permits_for_cama',
   dag=pipeline,
   csv_path='{{ ti.xcom_pull("make_lni_staging") }}/vw_permits_for_cama.csv',
   db_conn_id='databridge',
   db_table_name='gis_lni.vw_permits_for_cama',
   db_table_where='',
   db_timestamp=False,
)
#
#extract_inspections_for_cama =  GeopetlReadOperator(
#    task_id='read_inspections_for_cama',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/inspections_for_cama.csv',
#    db_conn_id='databridge2',
#    db_table_name='etl_user.vw_inspections_for_cama_w_pin_from_history_no_ts',
#    db_table_where='',
#)
#
upsert_permits_for_cama = GeopetlUpsertOperator(
    task_id='upsert_permits_for_cama',
    dag=pipeline,
    python_callable=update_db,
    db_conn_id='databridge2',
    db_table_name='lni.permits_for_cama',
    csv_path = '{{ ti.xcom_pull("make_lni_staging") }}/vw_permits_for_cama.csv',
    db_table_constraint = 'unq_permit_number_idx',
)
#
#write_inspections_for_cama = GeopetlWriteOperator(
#    task_id='write_inspections_for_cama',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/inspections_for_cama.csv',
#    db_conn_id='databridge2',
#    db_table_name='lni.inspections_for_cama',
#    db_table_where='',
#)
#
#update_permits_for_cama_hash = PythonOperator(
#    task_id='update_permits_for_cama_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'permits_for_cama', 'hash_field': 'etl_hash'},
#)
#
#update_inspections_for_cama_hash = PythonOperator(
#    task_id='update_inspections_for_cama_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'inspections_for_cama', 'hash_field': 'etl_hash'},
#)
#
#update_permits_for_cama_history = PythonOperator(
#    task_id='update_permits_for_cama_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'permits_for_cama', 'hash_field': 'etl_hash'},
#)
#
#update_inspections_for_cama_history = PythonOperator(
#    task_id='update_inspections_for_cama_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'lni', 'table_name': 'inspections_for_cama', 'hash_field': 'etl_hash'},
#)

extract_last_permits_for_cama_update_from_oracle = GeopetlReadOperator(
   task_id='read_last_permits_for_cama_update_from_oracle',
   dag=pipeline,
   csv_path='{{ ti.xcom_pull("make_lni_staging") }}/last_permits_for_cama_update_timestamp.csv',
   db_conn_id='databridge',
   db_table_name='gis_cama.property_permits',
   db_table_where='',
   db_sql=get_last_update_from_oracle_stmt.format(table_name='gis_cama.property_permits')
)

extract_last_inspections_for_cama_update_from_oracle = GeopetlReadOperator(
    task_id='read_last_inspections_for_cama_update_from_oracle',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/last_inspections_for_cama_update_timestamp.csv',
    db_conn_id='databridge',
    db_table_name='gis_cama.property_inspections',
    db_table_where='',
    db_sql=get_last_update_from_oracle_stmt.format(table_name='gis_cama.property_inspections')
)

extract_permits_for_cama_updates = PythonOperator(
    task_id='read_permits_for_cama_updates',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_permits_for_cama_table_name, 
               'stmt': select_permits_for_cama_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_lni_staging") }}/permits_for_cama_updates.csv', 
                    'update_date_csv_path':'{{ ti.xcom_pull("make_lni_staging") }}/last_permits_for_cama_update_timestamp.csv'},
)

extract_inspections_for_cama_updates = PythonOperator(
    task_id='read_inspections_for_cama_updates',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_inspections_for_cama_table_name, 
               'stmt': get_updates_from_db2_stmt.format(table_name=db2_inspections_for_cama_table_name, fields=inspections_update_fields), 
               'stmt_where': get_updates_from_db2_stmt_where
              },
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_lni_staging") }}/inspections_for_cama_updates.csv', 
                    'update_date_csv_path':'{{ ti.xcom_pull("make_lni_staging") }}/last_inspections_for_cama_update_timestamp.csv'
                   },
)

write_permits_for_cama_updates_to_db_oracle = GeopetlWriteOperator(
   task_id='write_permits_for_cama_updates',
   dag=pipeline,
   csv_path = '{{ ti.xcom_pull("make_lni_staging") }}/permits_for_cama_updates.csv',
   db_conn_id='databridge-cama',
   db_table_name='gis_cama.property_permits',
   db_table_where = '',
   append=True,
)

write_inspections_for_cama_updates_to_db_oracle = GeopetlWriteOperator(
    task_id='write_inspections_for_cama_updates',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_lni_staging") }}/inspections_for_cama_updates.csv',
    db_conn_id='databridge-cama',
    db_table_name='gis_cama.property_inspections',
    db_table_where = '',
    append=True,
)

# -----------------------------------------------------------------
# Carto updates:
permits_schema =  Variable.get('schemas') + 'db_lni_permits.json'
violations_schema = Variable.get('schemas') + 'db_lni_violations.json'
business_licenses_schema = Variable.get('schemas') + 'db_lni_business_licenses.json'

#write_carto_permits = CartoUpdateOperator(
#    task_id='write_carto_permits',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/permits.csv',
#    db_conn_id='carto_phl',
#    db_table_name='permits',
#    db_schema_json=permits_schema,
#    db_select_users=['publicuser', 'tileuser']
#)

#write_carto_violations = CartoUpdateOperator(
#    task_id='write_carto_violations',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/violations.csv',
#    db_conn_id='carto_phl',
#    db_table_name='violations',
#    db_schema_json=violations_schema,
#    db_select_users=['publicuser', 'tileuser']
#)

#write_carto_business_licenses = CartoUpdateOperator(
#    task_id='write_carto_business_licenses',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_lni_staging") }}/business_licenses.csv',
#    db_conn_id='carto_phl',
#    db_table_name='business_licenses',
#    db_schema_json=business_licenses_schema,
#    db_select_users=['publicuser', 'tileuser']
#)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_lni_staging") }}',
)

extract_permits.set_upstream(make_staging)
extract_permits.set_downstream(write_permits)
write_permits.set_downstream(update_permits_hash)
update_permits_hash.set_downstream(update_permits_history)
update_permits_history.set_downstream(cleanup)

extract_li_districts.set_upstream(make_staging)
extract_li_districts.set_downstream(write_li_districts)
write_li_districts.set_downstream(cleanup)

extract_violations.set_upstream(make_staging)
extract_violations.set_downstream(write_violations)
write_violations.set_downstream(cleanup)

#extract_eclipse_addressobjectid_mvw.set_upstream(make_staging)
#extract_eclipse_addressobjectid_mvw.set_downstream(write_eclipse_addressobjectid_mvw)
#write_eclipse_addressobjectid_mvw.set_downstream(cleanup)

extract_business_licenses.set_upstream(make_staging)
extract_business_licenses.set_downstream(write_business_licenses)
write_business_licenses.set_downstream(update_business_licenses_hash)
update_business_licenses_hash.set_downstream(update_business_licenses_history)
update_business_licenses_history.set_downstream(extract_last_inspections_for_cama_update_from_oracle)
extract_last_inspections_for_cama_update_from_oracle.set_downstream(extract_inspections_for_cama_updates)
extract_inspections_for_cama_updates.set_downstream(write_inspections_for_cama_updates_to_db_oracle)
write_inspections_for_cama_updates_to_db_oracle.set_downstream(cleanup)

# extract_parsed_addr.set_upstream(make_staging)
# extract_parsed_addr.set_downstream(write_parsed_addr)
# write_parsed_addr.set_downstream(cleanup)

extract_li_building_footprints.set_upstream(make_staging)
extract_li_building_footprints.set_downstream(write_li_building_footprints)
write_li_building_footprints.set_downstream(cleanup)

#extract_bp_pp_ep_issuedfinalinsp.set_upstream(make_staging)
#extract_bp_pp_ep_issuedfinalinsp.set_downstream(write_bp_pp_ep_issuedfinalinsp)
#write_bp_pp_ep_issuedfinalinsp.set_downstream(update_bp_pp_ep_issuedfinalinsp_hash)
#update_bp_pp_ep_issuedfinalinsp_hash.set_downstream(update_bp_pp_ep_issuedfinalinsp_history)
#update_bp_pp_ep_issuedfinalinsp_history.set_downstream(extract_last_permits_for_cama_update_from_oracle)
extract_last_permits_for_cama_update_from_oracle.set_upstream(make_staging)
extract_permits_for_cama.set_upstream(make_staging)
extract_permits_for_cama.set_downstream(upsert_permits_for_cama)
upsert_permits_for_cama.set_downstream(extract_permits_for_cama_updates)
extract_last_permits_for_cama_update_from_oracle.set_downstream(extract_permits_for_cama_updates)
extract_permits_for_cama_updates.set_downstream(write_permits_for_cama_updates_to_db_oracle)
write_permits_for_cama_updates_to_db_oracle.set_downstream(cleanup)
