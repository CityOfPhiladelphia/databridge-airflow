import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator, GeopetlUpsertOperator, GeopetlWriteOperatorDev
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.pin_sql_v2 import *
from datetime import datetime, timedelta
from airflow.models import Variable
import petl as etl

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': slack_failed_alert,
    'start_date': datetime(2020, 11, 10, 0, 0, 0),
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_process_property_transactions_v0', schedule_interval=None, default_args=default_args)

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
            stmt = stmt + ' ' + stmt_where if last_update_date else stmt
        else:
            stmt = stmt.format(last_update_date=last_update_date)
    print(stmt)
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    #pg_hook.bulk_dump(table_name, csv_path)
    pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)

def delete_temp_file(**kwargs):
    path = kwargs['templates_dict']['csv_path']
    filename = kwargs['templates_dict']['filename']
    os.remove(path + '/' + filename)


# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# update pin master:
pin_master_table_name='property.pin_master'
pin_master_view_name='property.vw_pin_master'
upsert_pin_master_stmt = upsert_pin_master_sql.format(pin_master_table_schema_name=pin_master_table_name, pin_master_view_schema_name=pin_master_view_name)
pin_master_update_stmt = '''
BEGIN;
{upsert_stmt}
delete from {pin_master_table_name} main where pin in (
	select pin from {pin_master_table_name}
	except
	select pin from {pin_master_view_name}
);
COMMIT;
'''.format(pin_master_table_name=pin_master_table_name, pin_master_view_name=pin_master_view_name, upsert_stmt=upsert_pin_master_stmt)

update_pin_master = PythonOperator(
    task_id='update_pin_master',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':pin_master_update_stmt},
)

extract_pin_master = GeopetlReadOperator(
    task_id='read_pin_master',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pin_master.csv',
    db_conn_id='databridge2',
    db_table_name='property.pin_master',
    db_table_where='',
)

write_databridge_pin_master = GeopetlWriteOperator(
    task_id='write_databridge_pin_master',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/pin_master.csv',
    db_conn_id='databridge-gsg',
    db_table_name='GIS_GSG.PIN_MASTER',
    db_table_where = '',
    append=False,
)

delete_temp_pin_master = PythonOperator(
    task_id='delete_temp_pin_master',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'pin_master.csv',},
)

# check cama for records that have been fully integrated and update status in stage transaction to "pinned"
update_er_stage_transaction_status_for_pinned_records = PythonOperator(
    task_id='update_er_stage_transaction_status_for_pinned_records',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_transaction_status_pinned_sql},
)

# upsert stage records:
upsert_records_into_er_stage_transaction = PythonOperator(
    task_id='upsert_records_into_er_stage_transaction',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_er_stage_transaction_sql},
)

upsert_records_into_er_stage_parcel = PythonOperator(
    task_id='upsert_records_into_er_stage_parcel',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_er_stage_parcel_sql},
)

upsert_records_into_er_stage_parties = PythonOperator(
    task_id='upsert_records_into_er_stage_parties',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_er_stage_parties_sql},
)

update_stage_transaction_status_deed_parsing = PythonOperator(
    task_id='update_stage_transaction_status_deed_parsing',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_stage_transaction_status_deed_parsing_sql},
)

# geocode new stage_parcel addresses:
get_er_stage_parcel_addresses_for_geocoding_stmt = '''
select distinct new_val->'concatenated_address' #>> '{}' as concatenated_address, current_timestamp as etl_read_timestamp
from audit.pin_history
where tabname = 'er_stage_parcel'
and etl_modified_timestamp > (Select coalesce(max(etl_read_timestamp),'1-1-2000') from property.er_stage_parcel_ais)
'''.format('{}')

extract_er_stage_props_for_geocoding = GeopetlReadOperator(
    task_id='extract_er_stage_props_for_geocoding',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/er_stage_props_addresses_for_geodcoding.csv',
    db_conn_id='databridge2',
    db_table_name='property.er_stage_parcel',
    db_table_where='',
    db_sql=get_er_stage_parcel_addresses_for_geocoding_stmt
)

geocode_er_stage_props = BashOperator(
    task_id='geocode_er_stage_props',
    bash_command='''batch_geocoder ais --input-file '{{ ti.xcom_pull("make_staging") }}/er_stage_props_addresses_for_geodcoding.csv' --ais-url http://api.phila.gov/ais/v1 --query-fields concatenated_address --ais-fields normalized,match_type,street_address,address_low,address_low_suffix,address_low_frac,address_high,street_predir,street_name,street_suffix,street_postdir,unit_type,unit_num,street_code,seg_id,zip_code,zip_4,usps_bldgfirm,usps_type,opa_account_num,dor_parcel_id,pwd_parcel_id,bin,li_parcel_id,li_address_key,eclipse_location_id,geocode_type,shape  --ais-key 5e0ee5872bd78c3b01b7913ab5083dc9 > '{{ ti.xcom_pull("make_staging") }}/geocoded_er_stage_props.csv' ''',
    dag=pipeline
)

upsert_geocoded_er_stage_props = GeopetlUpsertOperator(
    task_id='upsert_geocoded_er_stage_props',
    dag=pipeline,
    python_callable=update_db,
    db_conn_id='databridge2',
    db_table_name='property.er_stage_parcel_ais',
    csv_path = '{{ ti.xcom_pull("make_staging") }}/geocoded_er_stage_props.csv',
    db_table_constraint = 'er_stage_parcel_ais_concatenated_address_idx',
)

# run pin matching
er_stage_parcel_pin_matching_table_name = 'property.er_stage_parcel_pin_matching'
er_stage_parcel_pin_matching_view_name = 'property.vw_er_stage_parcel_pin_matching'
er_stage_parcel_table_name = 'property.er_stage_parcel'
upsert_er_stage_parcel_pin_matching_stmt = upsert_er_stage_parcel_pin_matching_sql.format(er_stage_parcel_pin_matching_table_schema_name=er_stage_parcel_pin_matching_table_name, er_stage_parcel_pin_matching_view_schema_name=er_stage_parcel_pin_matching_view_name)
delete_er_stage_parcel_pin_matching_stmt = delete_er_stage_parcel_pin_matching_sql.format(er_stage_parcel_pin_matching_table_schema_name=er_stage_parcel_pin_matching_table_name, er_stage_parcel_table_schema_name=er_stage_parcel_table_name)
update_er_stage_parcel_pin_matching_sql = '''
BEGIN;
{delete_er_stage_parcel_pin_matching_sql}
{upsert_er_stage_parcel_pin_matching_sql}
COMMIT;
'''.format(delete_er_stage_parcel_pin_matching_sql=delete_er_stage_parcel_pin_matching_stmt, upsert_er_stage_parcel_pin_matching_sql=upsert_er_stage_parcel_pin_matching_stmt)

update_er_stage_parcel_pin_matching = PythonOperator(
    task_id='update_er_stage_parcel_pin_matching',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_parcel_pin_matching_sql},
)

# update pin and pin_type in er_stage_parcel from pin matching
update_er_stage_parcel_pin_pin_type = PythonOperator(
    task_id='update_er_stage_parcel_pin_pin_type',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_parcel_pin_type_sql},
)

# usert property deeds new for CAMA:
property_deeds_new_table_name = 'cama.property_deeds_new'
property_deeds_new_view_name = 'cama.vw_property_deeds_new'
upsert_property_deeds_new_stmt = upsert_property_deeds_new_sql.format(property_deeds_new_table_schema_name=property_deeds_new_table_name, property_deeds_view_schema_name=property_deeds_new_view_name)
update_cama_property_deeds_new = PythonOperator(
    task_id='update_cama_property_deeds_new',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_property_deeds_new_stmt},
)

# send to CAMA

# non-queue 9 records:
get_last_update_from_target_stmt = '''select max(etl_modified_timestamp) as last_update from {table_name}'''
extract_last_update_from_property_deeds_new_non_queue_9 = GeopetlReadOperator(
    task_id='read_last_update_from_property_deeds_new_non_queue_9',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_property_deeds_new_non_queue_9_update_timestamp.csv',
    db_conn_id='databridge-cama',
    db_table_name='gis_cama.property_deeds_new',
    db_table_where='',
    db_sql=get_last_update_from_target_stmt.format(table_name='gis_cama.property_deeds_new')
)

extract_property_deeds_new_non_queue_9_updates = PythonOperator(
    task_id='read_property_deeds_new_non_queue_9_updates',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': 'cama.property_deeds_new', 'stmt': select_property_deeds_new_non_queue_9_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/property_deeds_new_non_queue_9_updates.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_property_deeds_new_non_queue_9_update_timestamp.csv'},
)

# append updated records to target table:
write_property_deeds_new_non_queue_9_updates = GeopetlWriteOperator(
    task_id='write_property_deeds_new_non_queue_9_updates',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}//property_deeds_new_non_queue_9_updates.csv',
    db_conn_id='databridge-cama',
    db_table_name='gis_cama.property_deeds_new',
    db_table_where = '',
    append=True,
)

delete_temp_property_deeds_new_non_queue_9_updates = PythonOperator(
    task_id='delete_temp_property_deeds_new_non_queue_9_updates',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'property_deeds_new_non_queue_9_updates.csv',},
)

# queue 9 records:
get_last_update_from_target_stmt = '''select max(etl_modified_timestamp) as last_update from {table_name}'''
extract_last_update_from_property_deeds_new_queue_9 = GeopetlReadOperator(
    task_id='read_last_update_from_property_deeds_new_queue_9',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_property_deeds_new_queue_9_update_timestamp.csv',
    db_conn_id='databridge-cama',
    db_table_name='gis_cama.property_deeds_new_queue_9_his',
    db_table_where='',
    db_sql=get_last_update_from_target_stmt.format(table_name='gis_cama.property_deeds_new_queue_9_his')
)

extract_property_deeds_new_queue_9_updates = PythonOperator(
    task_id='read_property_deeds_new_queue_9_updates',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': 'gis_cama.property_deeds_new_queue_9', 'stmt': select_property_deeds_new_queue_9_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/property_deeds_new_queue_9_updates.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_property_deeds_new_queue_9_update_timestamp.csv'},
)

# append updated records to target table:
write_property_deeds_new_queue_9_updates = GeopetlWriteOperator(
    task_id='write_property_deeds_new_queue_9_updates',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}//property_deeds_new_queue_9_updates.csv',
    db_conn_id='databridge-cama',
    db_table_name='gis_cama.property_deeds_new_queue_9_his',
    db_table_where = '',
    append=True,
)

delete_temp_property_deeds_new_queue_9_updates = PythonOperator(
    task_id='delete_temp_property_deeds_new_queue_9_updates',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'property_deeds_new_queue_9_updates.csv',},
)


# After sending, update_er_stage_parcel_sent_date and update_er_stage_transaction_status_opa_queue
update_er_stage_parcel_sent_date = PythonOperator(
    task_id='update_er_stage_parcel_sent_date',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_parcel_sent_date_sql},
)

update_er_stage_transaction_opa_queue = PythonOperator(
    task_id='update_er_stage_transaction_opa_queue',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_transaction_opa_queue_sql},
)


update_er_stage_transaction_status_opa_queue = PythonOperator(
    task_id='update_er_stage_transaction_status_opa_queue',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_transaction_status_opa_queue_sql},
)

#----------------------------------------------------------------------------------
from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators import SSHOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks import GeopetlHook


def query_oracle(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    print(stmt)
    geopetl_hook = GeopetlHook(db_conn_id=db_conn_id)
    conn = geopetl_hook.get_conn()
    cur = conn.cursor()
    cur.execute(stmt)
    conn.commit()

gisscripts = BaseHook.get_connection('gisscripts')
sshhook_instance = SSHHook(remote_host="citygeo-scripts1.phila.city",
                                        username="gisscripts",
                                        password=gisscripts.password,
                                        )

# reverse sync AGO queue 9 table from AGO to Databridge:

backwards_oracle_sync = SSHOperator(
                task_id="backwards_oracle_sync_AGO_queue_9_to_Databridge",
                dag=pipeline,
                command='C:/arcpy/python.exe C:/scripts/ago_to_databridge_backup_etl/ago_to_databridge_backup.py -ad PROPERTY_DEEDS_NEW_QUEUE_9 -d PROPERTY_DEEDS_NEW_QUEUE_9_AGO -a gis_cama',
                ssh_hook=sshhook_instance,
                )


# update Databridge queue_9 table from historical view & joining AGO editor info for publishing

update_opa_property_deeds_new_queue_9_pub_stmt = '''CALL GIS_CAMA.REFRESH_PROP_DEEDS_NEW_QUEUE_9()'''

update_opa_property_deeds_new_queue_9_pub = PythonOperator(
    task_id='update_opa_property_deeds_new_queue_9_pub',
    dag=pipeline,
    python_callable=query_oracle,
    op_kwargs={'db_conn_id':'databridge-cama', 'stmt': update_opa_property_deeds_new_queue_9_pub_stmt},
)

# publish to AGO

refresh_ago = SSHOperator(
                task_id="refresh_ago",
                dag=pipeline,
                command="C:/arcpy/python.exe C:/scripts/ago_updater/ago_update.py -d GIS_CAMA_property_deeds_new_queue_9 -o ago -p opa_suspense_queue_review_perms--enable-editing --preserve-editor-tracking --share-groups 'OPA Suspense Queue Review'",
                ssh_hook=sshhook_instance,
                )


# reverse sync AGO queue 9 review findings from AGO to Databridge:
backwards_q9_review_findings_sync = SSHOperator(
                task_id="backwards_queue_9_review_findings_AGO_to_Databridge",
                dag=pipeline,
                command='C:/arcpy/python.exe C:/scripts/ago_to_databridge_backup_etl/ago_to_databridge_backup.py -ad "OPA Suspense Queue Findings" -d QUEUE_9_REVIEW_FINDINGS -a gis_cama',
                ssh_hook=sshhook_instance,
                )


# update parcel_cleanup_pin_queue
parcel_cleanup_pin_queue_named_version_view = 'parcel_cleanup_pin_queue_evw'
extract_parcel_cleanup_pin_queue_stmt = ''' select base_address, brt_properties_base_address, brt_properties_unit_num, cama_address_match, cama_base_address, cama_owner_match, cama_owners, cama_title, cama_unit_num,
 concatenated_address, condo_name, condo_unit, document_date, document_type, dor_parcel_base_address, dor_parcel_cleanup_base_addr, dor_parcel_cleanup_unit_num, dor_parcel_unit_num, etl_modified_date, grantees, grantor_grantee_match,
 grantors, house_num_range, house_num_suffix, house_number, in_easement, in_row, instance, intersecting_seg_id, is_parent, match_type, no_address, no_mapreg, num_parcels_w_address, num_parcels_w_mapreg, number_of_parcels, objectid,
 opa_account_num, pcu_id, permit_description, permit_issue_date, pin, pin_type, pwd_parcel_base_address, pwd_parel_unit_num, recording_date, reg_map_id, research_tags, review_comments, review_partial_interest, review_pin, review_status,
 review_transaction_type, rtt_address_match, rtt_owner_match, rtt_summary_base_address, rtt_summary_owners, rtt_summary_unit_num, rtt_title, street_dir, street_dir_suffix, street_name, street_type, tags, tips_address_match, tips_base_address,
 tips_owner_match, tips_owners, tips_title, tips_unit_num, title, workflow_indicator, current_timestamp as etl_read_timestamp from {} '''.format(parcel_cleanup_pin_queue_named_version_view)

extract_tripoli_parcel_cleanup_pin_queue = PythonOperator(
 task_id='read_tripoli_parcel_cleanup_pin_queue',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'tripoli_dor', 'version_name': 'joint_editing', 'table_name': parcel_cleanup_pin_queue_named_version_view, 'stmt': extract_parcel_cleanup_pin_queue_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/tripoli_parcel_cleanup_pin_queue.csv'},
)

write_tripoli_parcel_cleanup_pin_queue = GeopetlWriteOperator(
    task_id='write_tripoli_parcel_cleanup_pin_queue',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/tripoli_parcel_cleanup_pin_queue.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_parcel_cleanup_pin_queue',
)

upsert_parcel_cleanup_pin_queue_update = PythonOperator(
    task_id='upsert_parcel_cleanup_pin_queue_update',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_parcel_cleanup_pin_queue_update_sql},
)

extract_parcel_cleanup_pin_queue_update = GeopetlReadOperator(
    task_id='read_parcel_cleanup_pin_queue_update',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}//parcel_cleanup_pin_queue_update.csv',
    db_conn_id='databridge2',
    db_table_name='dor.parcel_cleanup_pin_queue_update',
    db_table_where='',
)

write_parcel_cleanup_pin_queue_update = GeopetlWriteOperatorDev(
    task_id='write_parcel_cleanup_pin_queue_update',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}//parcel_cleanup_pin_queue_update.csv',
    db_conn_id='tripoli_test_dor',
    db_table_name='dor.parcel_cleanup_pin_queue_evw',
    db_table_where = '',
    version_name='joint_editing',
) 


# update pwd deed feed
update_deed_feed_for_pwd = PythonOperator(
    task_id='update_deed_feed_for_pwd',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_deed_feed_for_pwd_sql},
)

# get last update timestamp from target to use in target update:
get_last_update_from_target_stmt = '''select max(etl_modified_timestamp) as last_update from {table_name}'''
extract_last_update_from_tripoli_deed_feed_for_pwd = GeopetlReadOperator(
    task_id='read_last_update_from_tripoli_deed_feed_for_pwd',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_deed_feed_for_pwd_update_timestamp.csv',
    db_conn_id='tripoli_e3dbp_lni',
    db_table_name='lni.deed_feed_for_pwd',
    db_table_where='',
    db_sql=get_last_update_from_target_stmt.format(table_name='lni.deed_feed_for_pwd')
)

# get updated deed_feed_for_pwd records since last update:
db2_deed_feed_for_pwd_table_name = 'water.deed_feed_for_pwd'
get_deed_feed_for_pwd_updates_from_db2_stmt = '''
select dffp.*, prep.etl_modified_timestamp
from water.deed_feed_for_pwd dffp
inner join (
	select (new_val->'title')::text::integer as title, (new_val->'instance')::text::integer as instance, etl_modified_timestamp
	from audit.pin_history
	where tabname = 'deed_feed_for_pwd'
	and etl_modified_timestamp > '{last_update_date}'
) prep on prep.title = dffp.title and prep.instance = dffp.instance
order by title, instance
'''

extract_deed_feed_for_pwd_updates = PythonOperator(
    task_id='read_deed_feed_for_pwd_updates',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_deed_feed_for_pwd_table_name, 'stmt': get_deed_feed_for_pwd_updates_from_db2_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/deed_feed_for_pwd_updates.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_deed_feed_for_pwd_update_timestamp.csv'},
)

# append updated records to target table:
write_deed_feed_for_pwd_updates = GeopetlWriteOperator(
    task_id='write_deed_feed_for_pwd_updates',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}//deed_feed_for_pwd_updates.csv',
    db_conn_id='tripoli_e3dbp_lni',
    db_table_name='lni.deed_feed_for_pwd',
    db_table_where = '',
    append=True,
)

delete_temp_deed_feed_for_pwd = PythonOperator(
    task_id='delete_temp_deed_feed_for_pwd',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'deed_feed_for_pwd_updates.csv',},
)



## Extract parcel_cleanup_pin_queue:
#extract_dor_vw_parcel_cleanup_pin_queue = GeopetlReadOperator(
#    task_id='extract_dor_vw_parcel_cleanup_pin_queue',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/dor_vw_parcel_cleanup_pin_queue.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.vw_parcel_cleanup_pin_queue',
#    db_table_where='',
#)
#
#upsert_dor_vw_parcel_cleanup_pin_queue = GeopetlUpsertOperator(
#    task_id='upsert_dor_vw_parcel_cleanup_pin_queue',
#    dag=pipeline,
#    python_callable=update_db,
#    db_conn_id='tripoli-dor',
#    db_table_name='dor.parcel_cleanup_pin_queue_evw',
#    csv_path = '{{ ti.xcom_pull("make_staging") }}/dor_vw_parcel_cleanup_pin_queue.csv',
#    db_table_constraint = 'parcel_cleanup_pin_queue_title_instance_idx',
#    version_name='joint_editing'
#)
#
#delete_temp_dor_vw_parcel_cleanup_pin_queue = PythonOperator(
#    task_id='delete_temp_dor_vw_parcel_cleanup_pin_queue',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'dor_vw_parcel_cleanup_pin_queue.csv',},
#)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

make_staging.set_downstream(update_pin_master)
update_pin_master.set_downstream(extract_pin_master)
extract_pin_master.set_downstream(write_databridge_pin_master)
write_databridge_pin_master.set_downstream(delete_temp_pin_master)
delete_temp_pin_master.set_downstream(cleanup)
update_pin_master.set_downstream(upsert_records_into_er_stage_transaction)
update_pin_master.set_downstream(upsert_records_into_er_stage_parcel)
update_pin_master.set_downstream(upsert_records_into_er_stage_parties)
update_er_stage_transaction_status_for_pinned_records.set_upstream(update_pin_master)
update_er_stage_transaction_status_for_pinned_records.set_downstream(upsert_records_into_er_stage_transaction)
upsert_records_into_er_stage_transaction.set_downstream(update_stage_transaction_status_deed_parsing)
upsert_records_into_er_stage_parcel.set_downstream(extract_er_stage_props_for_geocoding)
upsert_records_into_er_stage_parties.set_downstream(update_er_stage_parcel_pin_matching)
update_stage_transaction_status_deed_parsing.set_downstream(update_er_stage_parcel_pin_matching)
update_er_stage_parcel_pin_matching.set_downstream(update_er_stage_parcel_pin_pin_type)

extract_er_stage_props_for_geocoding.set_downstream(geocode_er_stage_props)
geocode_er_stage_props.set_downstream(upsert_geocoded_er_stage_props)
upsert_geocoded_er_stage_props.set_downstream(update_er_stage_parcel_pin_matching)
update_er_stage_parcel_pin_matching.set_downstream(update_cama_property_deeds_new)
update_er_stage_parcel_pin_matching.set_downstream(update_deed_feed_for_pwd)
#update_er_stage_parcel_pin_matching.set_downstream(extract_dor_vw_parcel_cleanup_pin_queue)
#extract_dor_vw_parcel_cleanup_pin_queue.set_downstream(upsert_dor_vw_parcel_cleanup_pin_queue)
#upsert_dor_vw_parcel_cleanup_pin_queue.set_downstream(delete_temp_dor_vw_parcel_cleanup_pin_queue)
#delete_temp_dor_vw_parcel_cleanup_pin_queue.set_downstream(cleanup)
update_cama_property_deeds_new.set_downstream(cleanup)
update_deed_feed_for_pwd.set_downstream(extract_last_update_from_tripoli_deed_feed_for_pwd)
extract_last_update_from_tripoli_deed_feed_for_pwd.set_downstream(extract_deed_feed_for_pwd_updates)
extract_deed_feed_for_pwd_updates.set_downstream(write_deed_feed_for_pwd_updates)
write_deed_feed_for_pwd_updates.set_downstream(delete_temp_deed_feed_for_pwd)
delete_temp_deed_feed_for_pwd.set_downstream(cleanup)

make_staging.set_downstream(extract_last_update_from_property_deeds_new_non_queue_9)
make_staging.set_downstream(extract_last_update_from_property_deeds_new_queue_9)

update_cama_property_deeds_new.set_downstream(extract_property_deeds_new_non_queue_9_updates)
extract_property_deeds_new_non_queue_9_updates.set_upstream(extract_last_update_from_property_deeds_new_non_queue_9)
extract_property_deeds_new_non_queue_9_updates.set_downstream(write_property_deeds_new_non_queue_9_updates)

update_cama_property_deeds_new.set_downstream(extract_property_deeds_new_queue_9_updates)
extract_property_deeds_new_queue_9_updates.set_upstream(extract_last_update_from_property_deeds_new_queue_9)
extract_property_deeds_new_queue_9_updates.set_downstream(write_property_deeds_new_queue_9_updates)
write_property_deeds_new_queue_9_updates.set_downstream(delete_temp_property_deeds_new_queue_9_updates)

update_er_stage_parcel_sent_date.set_upstream(write_property_deeds_new_non_queue_9_updates)
update_er_stage_transaction_opa_queue.set_upstream(update_er_stage_parcel_sent_date)
update_er_stage_transaction_status_opa_queue.set_upstream(update_er_stage_transaction_opa_queue)
update_er_stage_transaction_status_opa_queue.set_downstream(delete_temp_property_deeds_new_queue_9_updates)
update_er_stage_transaction_status_opa_queue.set_downstream(delete_temp_property_deeds_new_non_queue_9_updates)

update_er_stage_parcel_sent_date.set_downstream(cleanup)
update_er_stage_transaction_status_opa_queue.set_downstream(cleanup)

extract_tripoli_parcel_cleanup_pin_queue.set_upstream(make_staging)
extract_tripoli_parcel_cleanup_pin_queue.set_downstream(write_tripoli_parcel_cleanup_pin_queue)
write_tripoli_parcel_cleanup_pin_queue.set_downstream(update_er_stage_parcel_pin_matching)
update_er_stage_parcel_pin_matching.set_downstream(upsert_parcel_cleanup_pin_queue_update)
upsert_parcel_cleanup_pin_queue_update.set_downstream(extract_parcel_cleanup_pin_queue_update)
extract_parcel_cleanup_pin_queue_update.set_downstream(write_parcel_cleanup_pin_queue_update)
write_parcel_cleanup_pin_queue_update.set_downstream(cleanup)

# Queue 9
make_staging >> backwards_oracle_sync >> update_opa_property_deeds_new_queue_9_pub >> refresh_ago >> cleanup
write_property_deeds_new_queue_9_updates >> update_opa_property_deeds_new_queue_9_pub
make_staging >> backwards_q9_review_findings_sync
update_cama_property_deeds_new.set_upstream(backwards_q9_review_findings_sync)
