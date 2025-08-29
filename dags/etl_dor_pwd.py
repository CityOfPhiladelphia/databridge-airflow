import os
import petl as etl
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
#from airflow.operators import GeopetlReadOperatorDev
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.pin_sql_v2 import *
from airflow.utils.pin_source_address_std import check_address_comps
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

pipeline = DAG('etl_dor_pwd_geopetl_v0', schedule_interval='15 7 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG


def delete_temp_file(**kwargs):
    path = kwargs['templates_dict']['csv_path']
    filename = kwargs['templates_dict']['filename']
    os.remove(path + '/' + filename)


def update_postgres(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)


def write_to_postgres(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    table_name=kwargs['table_name']
    csv_path=templates_dict['csv_path']
    rows = etl.fromcsv(csv_path, encoding='latin-1')
    header = rows[0]
    str_header = ''
    num_fields = len(header)
    for i, field in enumerate(header):
        if i < num_fields - 1:
            str_header += field + ', '
        else:
            str_header += field
    print(str_header)
    stmt_fmt = stmt.format(header=str_header, table_name=table_name)
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.copy_expert(stmt_fmt, csv_path)


def extract_from_postgres(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    stmt_where=kwargs.get('stmt_where', '')
    table_name=kwargs['table_name']
    update_date_file = templates_dict.get('update_date_csv_path', '')
    if update_date_file and stmt_where:
        last_update_date = etl.fromcsv(update_date_file)[1][0]
        stmt_where=stmt_where.format(last_update_date=last_update_date)
        stmt = stmt + ' ' + stmt_where if last_update_date else stmt
    print(stmt)
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_dor_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

# extract_rtt_summary = GeopetlReadOperator(
#    task_id='read_rtt_summary',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/rtt_summary.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.rtt_summary',
#    db_table_where='',
# )
#
#extract_databridge_dor_parcel = GeopetlReadOperator(
#    task_id='read_databridge_dor_parcel',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_dor_parcel.csv',
#    db_conn_id='databridge',
#    db_table_name='gis_dor.dor_parcel',
#    db_table_where='',
#)
#
#extract_databridge_dor_easement = GeopetlReadOperator(
#    task_id='read_databridge_dor_easement',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_dor_easement.csv',
#    db_conn_id='databridge',
#    db_table_name='gis_dor.dor_easement',
#    db_table_where='',
#)

extract_pwd_parcels = GeopetlReadOperator(
    task_id='read_pwd_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/pwd_parcels.csv',
    db_conn_id='databridge-v2-citygeo',
    db_table_name='viewer_pwd.pwd_parcels',
    db_table_where='',
)
# ----------------------------------------------------
# Write extracted files to Databridge-Raw
write_pwd_parcels = GeopetlWriteOperator(
    task_id='write_pwd_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/pwd_parcels.csv',
    db_conn_id='databridge2',
    db_table_name='water.databridge_pwd_parcels',
)

# Update hash
update_pwd_parcels_hash = PythonOperator(
    task_id='update_pwd_parcels_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'water', 'table_name': 'databridge_pwd_parcels', 'hash_field': 'etl_hash'},
)

## Update history
#update_pwd_parcels_history = PythonOperator(
#    task_id='update_pwd_parcels_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'water', 'table_name': 'databridge_pwd_parcels', 'hash_field': 'etl_hash'},
#)

#extract_pinned_pwd_parcels = GeopetlReadOperator(
#    task_id='read_pinned_pwd_parcels',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/pinned_pwd_parcels.csv',
#    db_conn_id='databridge2',
#    db_table_name='water.vw_databridge_pwd_parcels_pinned',
#    db_sql='select objectid, parcelid, tencode, address, owner1, owner2, bldg_code, bldg_desc, brt_id, num_brt, num_accounts, gross_area, pin, st_astext(shape) as shape from water.vw_databridge_pwd_parcels_pinned',
#)

#extract_tripoli_dor_parcel = GeopetlReadOperator(
#    task_id='read_tripoli_dor_parcel',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcel.csv',
#    db_conn_id='tripoli_dor',
#    db_table_name='dor.parcel_rev_evw',
##    db_table_name='dor.parcel',
#    db_table_where='st_isempty(shape) is false',
#    named_version='QAQC',
#)

extract_tripoli_dor_parcel = GeopetlReadOperator(
    task_id='read_tripoli_dor_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcel.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcel_rev_evw',
    db_sql='''select objectid,recsub,basereg,mapreg,parcel,recmap,stcod,house,suf,unit,stex,stdir,stnam,stdessuf,elev_flag,topelev,botelev,condoflag,matchflag,
            inactdate,orig_date,status,geoid,stdes,addr_source,addr_std,created_user,created_date,last_edited_user,last_edited_date,last_edit_reason,
            axiomaticid,comments,oid_bu,case when r = 1 then pin else Null end as pin,frac,unit_type,stex_frac,stex_suf,separated_rights,dor_review,opa_review,pwd_review,muniment_type,muniment_id,
            title,etl_read_timestamp,shape::text
            from(
            select objectid,recsub,basereg,mapreg,parcel,recmap,stcod,house,suf,unit,stex,stdir,stnam,stdessuf,elev_flag,topelev,botelev,condoflag,matchflag,
            inactdate,orig_date,status,geoid,stdes,addr_source,addr_std,created_user,created_date,last_edited_user,last_edited_date,last_edit_reason,'' as axiomaticid,
            comments,'' as oid_bu,pin,frac,unit_type,stex_frac,stex_suf,separated_rights,dor_review,opa_review,pwd_review,muniment_type,muniment_id,'' as title,
            current_timestamp as etl_read_timestamp,st_astext(shape) as shape, row_number() over(partition by pin order by last_edited_date nulls last) as r
            from dor.parcel_rev_evw where st_isempty(shape) is false and status in (1,3)
            ) prep
            union
            select objectid,recsub,basereg,mapreg,parcel,recmap,stcod,house,suf,unit,stex,stdir,stnam,stdessuf,elev_flag,topelev,botelev,condoflag,matchflag,
            inactdate,orig_date,status,geoid,stdes,addr_source,addr_std,created_user,created_date,last_edited_user,last_edited_date,last_edit_reason,'' as axiomaticid,
            comments, '' as oid_bu,Null as pin,frac,unit_type,stex_frac,stex_suf,separated_rights,dor_review,opa_review,pwd_review,muniment_type,muniment_id,'' as title,
            current_timestamp as etl_read_timestamp,st_astext(shape)::text as shape
            from dor.parcel_rev_evw where st_isempty(shape) is false and status not in (1,3)''',
#    db_table_name='dor.parcel',
#    db_table_where='st_isempty(shape) is false',
    named_version='QAQC',
)

extract_tripoli_dor_parcelhistory = GeopetlReadOperator(
    task_id='read_tripoli_dor_parcelhistory',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcelhistory.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcelhistory',
)


extract_tripoli_dor_pin_changes = GeopetlReadOperator(
    task_id='read_tripoli_dor_pin_changes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_pin_changes.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcel_cleanup_pin_changes',
    db_table_where='',
)

extract_tripoli_pcu_layer = GeopetlReadOperator(
    task_id='read_tripoli_pcu_layer',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_pcu_layer.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.pcu_layer',
    db_table_where='',
)

extract_dor_condominium = GeopetlReadOperator(
    task_id='read_tripoli_dor_condominium',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/condominium.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.condominium',
    db_table_where='',
    db_timestamp=False,
)

#parcel_cleanup_pin_queue_named_version_view = 'parcel_cleanup_pin_queue_evw'
#extract_parcel_cleanup_pin_queue_stmt = ''' select base_address, brt_properties_base_address, brt_properties_unit_num, cama_address_match, cama_base_address, cama_owner_match, cama_owners, cama_title, cama_unit_num, 
# concatenated_address, condo_name, condo_unit, document_date, document_type, dor_parcel_base_address, dor_parcel_cleanup_base_addr, dor_parcel_cleanup_unit_num, dor_parcel_unit_num, etl_modified_date, grantees, grantor_grantee_match, 
# grantors, house_num_range, house_num_suffix, house_number, in_easement, in_row, instance, intersecting_seg_id, is_parent, match_type, no_address, no_mapreg, num_parcels_w_address, num_parcels_w_mapreg, number_of_parcels, objectid,
# opa_account_num, pcu_id, permit_description, permit_issue_date, pin, pin_type, pwd_parcel_base_address, pwd_parel_unit_num, recording_date, reg_map_id, research_tags, review_comments, review_partial_interest, review_pin, review_status, 
# review_transaction_type, rtt_address_match, rtt_owner_match, rtt_summary_base_address, rtt_summary_owners, rtt_summary_unit_num, rtt_title, street_dir, street_dir_suffix, street_name, street_type, tags, tips_address_match, tips_base_address, 
# tips_owner_match, tips_owners, tips_title, tips_unit_num, title, workflow_indicator, current_timestamp as etl_read_timestamp from {} '''.format(parcel_cleanup_pin_queue_named_version_view)
#
#extract_tripoli_parcel_cleanup_pin_queue = PythonOperator(
# task_id='read_tripoli_parcel_cleanup_pin_queue',
#    dag=pipeline,
#    python_callable=extract_from_postgres,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'tripoli_dor', 'version_name': 'joint_editing', 'table_name': parcel_cleanup_pin_queue_named_version_view, 'stmt': extract_parcel_cleanup_pin_queue_stmt},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}/tripoli_parcel_cleanup_pin_queue.csv'},
#)

#parcel_cleanup_missing_entities_named_version_view = 'parcel_cleanup_missing_entities_evw'
#extract_parcel_cleanup_missing_entities_stmt = ''' select objectid, st_astext(shape)::text as shape, created_user, created_date,last_edited_user, last_edited_date, m_pin, entity_type, comments, current_timestamp as etl_read_timestamp from {} '''.format(parcel_cleanup_pin_queue_named_version_view)
##extract_parcel_cleanup_missing_entities_stmt = '''select m_pin, entity_type, comments, current_timestamp as etl_read_timestamp from {}'''.format(parcel_cleanup_missing_entities_named_version_view)
#extract_tripoli_parcel_cleanup_missing_entities = PythonOperator(
# task_id='read_tripoli_parcel_cleanup_missing_entities',
#    dag=pipeline,
#    python_callable=extract_from_postgres,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'tripoli_dor', 'version_name': 'joint_editing', 'table_name': parcel_cleanup_missing_entities_named_version_view, 'stmt': extract_parcel_cleanup_missing_entities_stmt},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}/tripoli_parcel_cleanup_missing_entities.csv'},
#)

extract_tripoli_parcel_cleanup_missing_entities = GeopetlReadOperator(
    task_id='read_tripoli_parcel_cleanup_missing_entities',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_parcel_cleanup_missing_entities.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcel_cleanup_missing_entities',
    db_table_where='',
)

# eagle recorder pin, wfi, partial interest status reports:
extract_dor_er_deeds_no_pin_report = GeopetlReadOperator(
    task_id='read_dor_er_deeds_no_pin_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_pin_report.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_deeds_new_unpopulated_pin',
    db_table_where='',
)

extract_dor_er_deeds_no_pin_no_misc_report = GeopetlReadOperator(
    task_id='read_dor_er_deeds_no_pin_no_misc_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_pin_no_misc_report.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_deeds_new_unpopulated_pin_no_misc',
    db_table_where='',
)

extract_dor_er_deeds_no_wfi_report = GeopetlReadOperator(
    task_id='read_dor_er_deeds_no_wfi_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_wfi_report.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_deeds_new_unpopulated_transaction_type',
    db_table_where='',
)

extract_dor_er_deeds_no_wfi_no_misc_report = GeopetlReadOperator(
    task_id='read_dor_er_deeds_no_wfi_no_misc_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_wfi_no_misc_report.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_deeds_new_unpopulated_transaction_type_no_misc',
    db_table_where='',
)

extract_dor_er_deeds_no_p_int_report = GeopetlReadOperator(
    task_id='read_dor_er_deeds_no_p_int_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_p_int_report.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_deeds_new_unpopulated_partial_interest',
    db_table_where='',
)

extract_dor_er_deeds_no_p_int_no_misc_report = GeopetlReadOperator(
    task_id='read_dor_er_deeds_no_p_int_no_misc_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_p_int_no_misc_report.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_deeds_new_unpopulated_partial_interest_no_misc',
    db_table_where='',
)

extract_dor_er_deeds_erroneous_pin_report = GeopetlReadOperator(
    task_id='read_dor_er_deeds_erroneous_pin_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_erroneous_pin_report.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_deeds_new_erroneous_pin',
    db_table_where='',
)


# ----------------------------------------------------
# Write extracted files to Databridge

write_dor_er_deeds_no_pin_report = GeopetlWriteOperator(
    task_id='write_dor_er_deeds_no_pin_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_pin_report.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.new_er_deeds_no_pin',
)

write_dor_er_deeds_no_pin_no_misc_report = GeopetlWriteOperator(
    task_id='write_dor_er_deeds_no_pin_no_misc_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_pin_no_misc_report.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.new_er_deeds_no_pin_no_misc',
)

write_dor_er_deeds_no_wfi_report = GeopetlWriteOperator(
    task_id='write_dor_er_deeds_no_wfi_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_wfi_report.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.new_er_deeds_no_wfi',
)

write_dor_er_deeds_no_wfi_no_misc_report = GeopetlWriteOperator(
    task_id='write_dor_er_deeds_no_wfi_no_misc_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_wfi_no_misc_report.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.new_er_deeds_no_wfi_no_misc',
)

write_dor_er_deeds_no_p_int_report = GeopetlWriteOperator(
    task_id='write_dor_er_deeds_no_p_int_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_p_int_report.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.new_er_deeds_no_p_int',
)

write_dor_er_deeds_no_p_int_no_misc_report = GeopetlWriteOperator(
    task_id='write_dor_er_deeds_no_p_int_no_misc_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_no_p_int_no_misc_report.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.new_er_deeds_no_p_int_no_misc',
)

write_dor_er_deeds_erroneous_pin_report = GeopetlWriteOperator(
    task_id='write_dor_er_deeds_erroneous_pin_report',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_er_deeds_erroneous_pin_report.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.new_er_deeds_erroneous_pins',
)

# write_rtt_summary = GeopetlWriteOperator(
#    task_id='write_rtt_summary',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/rtt_summary.csv',
#    db_conn_id='databridge-v2-citygeo',
#    db_table_name='citygeo.rtt_summary',
# )

#
#write_dadtabridge_dor_parcel = GeopetlWriteOperator(
#    task_id='write_databridge_dor_parcel',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_dor_parcel.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.databridge_dor_parcel',
#)
#
#write_databridge_dor_easement = GeopetlWriteOperator(
#    task_id='write_databridge_dor_easement',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_dor_easement.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.databridge_dor_easement',
#)
#update_pinned_pwd_parcels_db2 = GeopetlWriteOperator(
#    task_id='update_pinned_pwd_parcels_db2',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/pinned_pwd_parcels.csv',
#    db_conn_id='databridge2',
#    db_table_name='water.databridge_pwd_parcels_pinned',
#)


#write_pinned_pwd_parcels = GeopetlWriteOperator(
#    task_id='write_pinned_pwd_parcels',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/pinned_pwd_parcels.csv',
#    db_conn_id='databridge-water',
#    db_table_name='gis_water.pwd_parcels_pinned',
#)

write_tripoli_dor_parcel = GeopetlWriteOperator(
    task_id='write_tripoli_dor_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcel.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_dor_parcel',
)

write_tripoli_dor_parcelhistory = GeopetlWriteOperator(
    task_id='write_tripoli_dor_parcelhistory',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcelhistory.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_dor_parcelhistory',
)

write_tripoli_dor_pin_changes = GeopetlWriteOperator(
    task_id='write_tripoli_dor_pin_changes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_pin_changes.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_dor_pin_changes',
)

write_tripoli_pcu_layer = GeopetlWriteOperator(
    task_id='write_tripoli_pcu_layer',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_pcu_layer.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_pcu_layer',
)

#write_tripoli_parcel_cleanup_pin_queue = GeopetlWriteOperator(
#    task_id='write_tripoli_parcel_cleanup_pin_queue',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_parcel_cleanup_pin_queue.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.tripoli_parcel_cleanup_pin_queue',
#)

write_tripoli_parcel_cleanup_missing_entities = GeopetlWriteOperator(
    task_id='write_tripoli_parcel_cleanup_missing_entities',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_parcel_cleanup_missing_entities.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_parcel_cleanup_missing_entities',
)

write_dor_condominium = GeopetlWriteOperator(
    task_id='write_dor_condominium',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/condominium.csv',
    db_conn_id='databridge2',
    db_table_name='dor.databridge_condominium',
)


#------------------------------------------------------------------
# Remove temp files

# delete_temp_rtt_summary = PythonOperator(
#    task_id='delete_temp_rtt_summary',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'rtt_summary.csv',},
# )


delete_temp_tripoli_dor_parcel = PythonOperator(
    task_id='delete_temp_tripoli_dor_parcel',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_dor_parcel.csv',},
)

delete_temp_tripoli_dor_parcelhistory = PythonOperator(
    task_id='delete_temp_tripoli_dor_parcelhistory',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_dor_parcelhistory.csv',},
)

delete_temp_pwd_parcels = PythonOperator(
    task_id='delete_temp_pwd_parcels',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'pwd_parcels.csv',},
)

delete_temp_tripoli_dor_pin_changes = PythonOperator(
    task_id='delete_temp_tripoli_dor_pin_changes',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_dor_pin_changes.csv',},
)

delete_temp_tripoli_pcu_layer = PythonOperator(
    task_id='delete_temp_tripoli_pcu_layer',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_pcu_layer.csv',},
)

#delete_temp_tripoli_parcel_cleanup_pin_queue = PythonOperator(
#    task_id='delete_temp_tripoli_parcel_cleanup_pin_queue',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_parcel_cleanup_pin_queue.csv',},
#)

delete_temp_tripoli_parcel_cleanup_missing_entities = PythonOperator(
    task_id='delete_temp_tripoli_parcel_cleanup_missing_entities',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_parcel_cleanup_missing_entities.csv',},
)


# -----------------------------------------------------------------
# Update hashes
#
update_tripoli_dor_parcel_hash = PythonOperator(
    task_id='update_tripoli_dor_parcel_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_parcel', 'hash_field': 'etl_hash'},
)

update_tripoli_dor_pin_changes_hash = PythonOperator(
    task_id='update_tripoli_dor_pin_changes_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_pin_changes', 'hash_field': 'etl_hash'},
)

update_tripoli_pcu_layer_hash = PythonOperator(
    task_id='update_tripoli_pcu_layer_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_pcu_layer', 'hash_field': 'etl_hash'},
)
#
#update_tripoli_parcel_cleanup_pin_queue_hash = PythonOperator(
#    task_id='update_tripoli_parcel_cleanup_pin_queue_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_parcel_cleanup_pin_queue', 'hash_field': 'etl_hash'},
#)

update_tripoli_parcel_cleanup_missing_entities_hash = PythonOperator(
    task_id='update_tripoli_parcel_cleanup_missing_entities_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_parcel_cleanup_missing_entities', 'hash_field': 'etl_hash'},
)
#
#
# -----------------------------------------------------------------
# Update histories
#
update_tripoli_dor_parcel_history = PythonOperator(
    task_id='update_tripoli_dor_parcel_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_parcel', 'hash_field': 'etl_hash'},
)

update_tripoli_dor_pin_changes_history = PythonOperator(
    task_id='update_tripoli_dor_pin_changes_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_pin_changes', 'hash_field': 'etl_hash'},
)

update_tripoli_pcu_layer_history = PythonOperator(
    task_id='update_tripoli_pcu_layer_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_pcu_layer', 'hash_field': 'etl_hash'},
)

# Update dor_parcel_geom_compare report:
db2_dor_parcel_geom_compare_table_name = 'dor.tripoli_dor_parcel_geom_compare'
db2_tripoli_dor_parcel_table_name = 'dor.tripoli_dor_parcel'
update_dor_parcel_geom_compare_stmt = '''
BEGIN;
truncate table {db2_dor_parcel_geom_compare_table_name};
insert into {db2_dor_parcel_geom_compare_table_name} (select objectid, pin, mapreg, stcod, house, suf, unit, stex, stdir, stnam, stdessuf, stdes, stex_frac, stex_suf, shape from {db2_tripoli_dor_parcel_table_name} where status in (1,3));
COMMIT;
'''.format(db2_dor_parcel_geom_compare_table_name=db2_dor_parcel_geom_compare_table_name, db2_tripoli_dor_parcel_table_name=db2_tripoli_dor_parcel_table_name)

update_tripoli_dor_parcel_geom_compare= PythonOperator(
    task_id='update_tripoli_dor_parcel_geom_compare',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_dor_parcel_geom_compare_stmt},
)

db2_dor_parcel_geom_compare_report_table_name = 'dor.tripoli_dor_parcel_geom_changes'
db2_tripoli_dor_parcel_table_name = 'dor.tripoli_dor_parcel'
db2_dor_parcel_geom_compare_view_name = 'dor.vw_tripoli_dor_parcel_new_geom_changes'
update_dor_parcel_geom_changes_stmt = '''
BEGIN;
insert into {db2_dor_parcel_geom_compare_report_table_name} (shape, etl_timestamp) (select new.shape, prep.etl_timestamp
from {db2_tripoli_dor_parcel_table_name} new
inner join {db2_dor_parcel_geom_compare_view_name} prep on prep.shape = new.shape
);
COMMIT;
'''.format(db2_dor_parcel_geom_compare_report_table_name=db2_dor_parcel_geom_compare_report_table_name, db2_tripoli_dor_parcel_table_name=db2_tripoli_dor_parcel_table_name, db2_dor_parcel_geom_compare_view_name=db2_dor_parcel_geom_compare_view_name)

update_tripoli_dor_parcel_geom_changes = PythonOperator(
    task_id='update_dor_parcel_geom_changes',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_dor_parcel_geom_changes_stmt},
)

extract_last_update_from_tripoli_planning_dor_parcel_geom_changes = GeopetlReadOperator(
    task_id='read_last_update_from_tripoli_planning_dor_parcel_geom_changes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/last_geom_change_update_timestamp.csv',
    db_conn_id='tripoli-planning',
    db_table_name='dor_parcel_geom_changes',
    db_table_where='',
    db_sql='select max(etl_timestamp) as last_update from planning.dor_parcel_geom_changes'
)

extract_dor_parcel_geom_changes_for_planning_for_revenue = PythonOperator(
    task_id='read_dor_parcel_geom_changes_for_planning',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': 'dor.tripoli_dor_parcel_geom_changes', 'stmt': 'select st_astext(shape) as shape, etl_timestamp from dor.tripoli_dor_parcel_geom_changes', 'stmt_where': '''where etl_timestamp > '{last_update_date}' '''},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}/dor_parcel_geom_change_updates_for_planning.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_dor_staging") }}/last_geom_change_update_timestamp.csv'},
)

#write to tripoli
write_dor_parcel_geom_change_updates_for_planning_to_tripoli = GeopetlWriteOperator(
    task_id='write_dor_parcel_geom_change_updates_for_planning_to_tripoli',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_dor_staging") }}/dor_parcel_geom_change_updates_for_planning.csv',
    db_conn_id='tripoli-planning',
    db_table_name='planning.dor_parcel_geom_changes',
    db_table_where = '',
    append=True,
)

# -----------------------------------------------------------------
# Trigger dor-pwd-overlap-analysis
db2_parcel_overlap_analysis_view_name='dor.vw_pinned_dor_pwd_overlap_analysis_pcu'
db2_parcel_overlap_analysis_table_name='dor.pinned_dor_pwd_overlap_analysis'

update_dor_pwd_overlap_analysis_stmt = '''
BEGIN;
truncate table {parcel_overlap_analysis_table_name};
insert into {parcel_overlap_analysis_table_name} (select * from {parcel_overlap_analysis_view_name});
COMMIT;
'''.format(parcel_overlap_analysis_table_name=db2_parcel_overlap_analysis_table_name, parcel_overlap_analysis_view_name=db2_parcel_overlap_analysis_view_name)

update_dor_pwd_overlap_analysis = PythonOperator(
    task_id='update_dor_pwd_overlap_analysis',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_dor_pwd_overlap_analysis_stmt},
)

# update parcel_analysis
db2_parcel_analysis_view_name = 'dor.vw_parcel_analysis'
db2_parcel_analysis_table_name = 'dor.parcel_analysis'
update_dor_parcel_analysis_stmt = '''
BEGIN;
truncate table {parcel_analysis_table_name};
insert into {parcel_analysis_table_name} (select * from {parcel_analysis_view_name});
COMMIT;
'''.format(parcel_analysis_table_name=db2_parcel_analysis_table_name, parcel_analysis_view_name=db2_parcel_analysis_view_name)

update_dor_parcel_analysis = PythonOperator(
    task_id='update_dor_parcel_analysis',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_dor_parcel_analysis_stmt},
)

extract_dor_parcel_analysis = GeopetlReadOperator(
    task_id='read_dor_parcel_analysis',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_parcel_analysis.csv',
    db_conn_id='databridge2',
    db_table_name=db2_parcel_analysis_table_name,
    db_table_where='',
)

write_dor_parcel_analysis_to_tripoli = GeopetlWriteOperator(
    task_id='write_dor_parcel_analysis_to_tripoli',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/dor_parcel_analysis.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcel_analysis',
)

delete_temp_dor_parcel_analysis = PythonOperator(
    task_id='delete_temp_dor_parcel_analysis',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'dor_parcel_analysis.csv',},
)

########################################
# Analyses incorporating pin changes:
#   update dor_parcel_pin_changes_geom
db2_dor_parcel_pin_changes_geom_view_name = 'dor.vw_dor_parcel_pin_changes_geom'
db2_dor_parcel_pin_changes_geom_table_name = 'dor.dor_parcel_pin_changes_geom'
update_dor_parcel_pin_changes_geom_stmt = '''
BEGIN;
truncate table {db2_dor_parcel_pin_changes_geom_table_name};
insert into {db2_dor_parcel_pin_changes_geom_table_name} (select * from {db2_dor_parcel_pin_changes_geom_view_name});
COMMIT;
'''.format(db2_dor_parcel_pin_changes_geom_table_name=db2_dor_parcel_pin_changes_geom_table_name, db2_dor_parcel_pin_changes_geom_view_name=db2_dor_parcel_pin_changes_geom_view_name)

update_dor_parcel_pin_changes_geom = PythonOperator(
    task_id='update_dor_parcel_pin_changes_geom',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_dor_parcel_pin_changes_geom_stmt},
)

#
# -----------------------------------------------------------------
# Extract and perform standardization report for distinct street_addresses from pin_source_addresses
pin_source_address_table_name = 'property.pin_source_address'
pin_source_address_view_name = 'property.vw_pin_source_address'

update_pin_source_address_from_view_stmt = '''
BEGIN;
truncate table {pin_source_address_table_name};
insert into {pin_source_address_table_name} (select * from {pin_source_address_view_name});
COMMIT;
'''.format(pin_source_address_table_name=pin_source_address_table_name, pin_source_address_view_name=pin_source_address_view_name)

update_pin_source_address_from_view = PythonOperator(
    task_id='update_pin_source_address_from_view',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_pin_source_address_from_view_stmt},
)

read_distinct_pin_source_address_stmt = '''
select distinct street_address from {pin_source_address_table_name}
'''.format(pin_source_address_table_name=pin_source_address_table_name)

extract_distinct_pin_source_addresses = GeopetlReadOperator(
    task_id='read_distinct_pin_source_addresses',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/distinct_pin_source_addresses.csv',
    db_conn_id='databridge2',
    db_table_name=pin_source_address_table_name,
    db_table_where='',
    db_sql=read_distinct_pin_source_address_stmt
)

standardize_address_comps = PythonOperator(
    task_id='standardize_address_comps',
    dag=pipeline,
    python_callable=check_address_comps,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}','infile_name': 'distinct_pin_source_addresses.csv', 'outfile_suffix':'_std'},
)

# Write to databridge2:
db2_source_address_std_table_name = 'property.pin_source_address_std'
write_pin_source_addresses_std_stmt = '''
BEGIN;
truncate table {table_name};
COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true);
COMMIT;
'''

write_std_pin_source_address_comps = PythonOperator(
    task_id='write_std_pin_source_address_comps',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_source_address_std_table_name, 'stmt': write_pin_source_addresses_std_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}/distinct_pin_source_addresses_std.csv'},
)

pin_source_address_plus_std_table_name = 'property.pin_source_address_plus_std'
pin_source_address_plus_std_view_name = 'property.vw_pin_source_address_plus_std'
pin_source_address_plus_std_temp_table_name = 'temp_pin_source_address_plus_std'
upsert_pin_source_address_plus_std_stmt = upsert_pin_source_address_plus_std_sql.format(pin_source_address_plus_std_table_schema_name=pin_source_address_plus_std_table_name, pin_source_address_plus_std_view_schema_name=pin_source_address_plus_std_view_name)
update_pin_source_address_plus_std_stmt = '''
BEGIN;
create temp table {pin_source_address_plus_std_temp_table_name} as select * from {pin_source_address_plus_std_view_name};
{upsert_stmt};
delete from {pin_source_address_plus_std_table_name} main where (pin, address_source) in (
        select pin, address_source from {pin_source_address_plus_std_temp_table_name}
        except
        select pin, address_source from {pin_source_address_plus_std_temp_table_name}
);
COMMIT;
'''.format(pin_source_address_plus_std_table_name=pin_source_address_plus_std_table_name, pin_source_address_plus_std_view_name=pin_source_address_plus_std_view_name, pin_source_address_plus_std_temp_table_name=pin_source_address_plus_std_temp_table_name, upsert_stmt=upsert_pin_source_address_plus_std_stmt)

# update pin_source_address_plus_std table from view using upsert:
update_pin_source_address_plus_std_from_view = PythonOperator(
    task_id='update_pin_source_address_plus_std_from_view',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_pin_source_address_plus_std_stmt},
)

# # extract updated table:
# extract_pin_source_address_plus_std = GeopetlReadOperator(
#     task_id='read_pin_source_address_plus_std',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_dor_staging") }}/pin_source_address_plus_std.csv',
#     db_conn_id='databridge2',
#     db_table_name='property.pin_source_address_plus_std',
#     db_table_where='',
# )

# write_pin_source_address_plus_std = GeopetlWriteOperator(
#     task_id='write_pin_source_address_plus_std',
#     dag=pipeline,
#     csv_path = '{{ ti.xcom_pull("make_dor_staging") }}/pin_source_address_plus_std.csv',
#     db_conn_id='databridge-v2-citygeo',
#     db_table_name='citygeo.pin_source_address_std',
#     db_table_where = '',
#     append=False,
# )

# delete_temp_pin_source_address_plus_std = PythonOperator(
#     task_id='delete_temp_pin_source_address_plus_std',
#     dag=pipeline,
#     python_callable=delete_temp_file,
#     provide_context=True,
#     templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'pin_source_address_plus_std.csv',},
# )

##########################
# Update property.parcel #
##########################
# upsert property parcel table from view:
upsert_property_parcel_stmt = upsert_property_parcel_sql
upsert_property_parcel = PythonOperator(
    task_id='upsert_property_parcel',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': upsert_property_parcel_stmt},
)

# Update property parcel status for inactives:
update_property_parcel_status_for_inactives_stmt = update_property_parcel_status_for_inactives_sql
update_property_parcel_status_for_inactives = PythonOperator(
    task_id='update_property_parcel_status_for_inactives',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_property_parcel_status_for_inactives_stmt},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_dor_staging") }}',
)

# extract_rtt_summary.set_upstream(make_staging)
# extract_rtt_summary.set_downstream(write_rtt_summary)
# write_rtt_summary.set_downstream(delete_temp_rtt_summary)
# write_rtt_summary.set_downstream(cleanup)
#
#extract_databridge_dor_parcel.set_upstream(make_staging)
#extract_databridge_dor_parcel.set_downstream(write_databridge_dor_parcel)
#write_databridge_dor_parcel.set_downstream(cleanup)
#
#extract_databridge_dor_easement.set_upstream(make_staging)
#extract_databridge_dor_easement.set_downstream(write_databridge_dor_easement)
#write_databridge_dor_easement.set_downstream(cleanup)

extract_dor_er_deeds_no_pin_report.set_upstream(make_staging)
extract_dor_er_deeds_no_pin_no_misc_report.set_upstream(make_staging)
extract_dor_er_deeds_no_wfi_report.set_upstream(make_staging)
extract_dor_er_deeds_no_wfi_no_misc_report.set_upstream(make_staging)
extract_dor_er_deeds_no_p_int_report.set_upstream(make_staging)
extract_dor_er_deeds_no_p_int_no_misc_report.set_upstream(make_staging)
extract_dor_er_deeds_no_pin_report.set_downstream(write_dor_er_deeds_no_pin_report)
extract_dor_er_deeds_no_pin_no_misc_report.set_downstream(write_dor_er_deeds_no_pin_no_misc_report)
extract_dor_er_deeds_no_wfi_report.set_downstream(write_dor_er_deeds_no_wfi_report)
extract_dor_er_deeds_no_wfi_no_misc_report.set_downstream(write_dor_er_deeds_no_wfi_no_misc_report)
extract_dor_er_deeds_no_p_int_report.set_downstream(write_dor_er_deeds_no_p_int_report)
extract_dor_er_deeds_no_p_int_no_misc_report.set_downstream(write_dor_er_deeds_no_p_int_no_misc_report)
extract_dor_er_deeds_erroneous_pin_report.set_upstream(make_staging)
extract_dor_er_deeds_erroneous_pin_report.set_downstream(write_dor_er_deeds_erroneous_pin_report)
write_dor_er_deeds_no_pin_report.set_downstream(cleanup)
write_dor_er_deeds_no_pin_no_misc_report.set_downstream(cleanup)
write_dor_er_deeds_no_wfi_report.set_downstream(cleanup)
write_dor_er_deeds_no_wfi_no_misc_report.set_downstream(cleanup)
write_dor_er_deeds_no_p_int_report.set_downstream(cleanup)
write_dor_er_deeds_no_p_int_no_misc_report.set_downstream(cleanup)
write_dor_er_deeds_erroneous_pin_report.set_downstream(cleanup)


extract_pwd_parcels.set_upstream(make_staging)
extract_pwd_parcels.set_downstream(write_pwd_parcels)
write_pwd_parcels.set_downstream(delete_temp_pwd_parcels)
write_pwd_parcels.set_downstream(update_pwd_parcels_hash)
#update_pwd_parcels_hash.set_downstream(update_pwd_parcels_history)
write_pwd_parcels.set_downstream(update_dor_pwd_overlap_analysis)
#extract_pinned_pwd_parcels.set_upstream(write_pwd_parcels)
#extract_pinned_pwd_parcels.set_downstream(write_pinned_pwd_parcels)
#extract_pinned_pwd_parcels.set_downstream(update_pinned_pwd_parcels_db2)
#update_pinned_pwd_parcels_db2.set_downstream(update_dor_pwd_overlap_analysis)
update_dor_pwd_overlap_analysis.set_downstream(update_dor_parcel_analysis)
update_dor_parcel_analysis.set_downstream(extract_dor_parcel_analysis)
extract_dor_parcel_analysis.set_downstream(write_dor_parcel_analysis_to_tripoli)
write_dor_parcel_analysis_to_tripoli.set_downstream(delete_temp_dor_parcel_analysis)
delete_temp_dor_parcel_analysis.set_downstream(cleanup)



extract_tripoli_pcu_layer.set_upstream(make_staging)
extract_tripoli_pcu_layer.set_downstream(write_tripoli_pcu_layer)
write_tripoli_pcu_layer.set_downstream(update_tripoli_pcu_layer_hash)
write_tripoli_pcu_layer.set_downstream(delete_temp_tripoli_pcu_layer)
update_tripoli_pcu_layer_hash.set_downstream(update_tripoli_pcu_layer_history)
update_tripoli_pcu_layer_history.set_downstream(cleanup)
write_tripoli_pcu_layer.set_downstream(update_dor_pwd_overlap_analysis)

extract_tripoli_dor_parcel.set_upstream(make_staging)
extract_tripoli_dor_parcel.set_downstream(write_tripoli_dor_parcel)
write_tripoli_dor_parcel.set_downstream(update_tripoli_dor_parcel_hash)
write_tripoli_dor_parcel.set_downstream(delete_temp_tripoli_dor_parcel)
update_tripoli_dor_parcel_hash.set_downstream(update_tripoli_dor_parcel_history)
write_tripoli_dor_parcel.set_downstream(update_dor_pwd_overlap_analysis)
update_tripoli_dor_parcel_history.set_downstream(update_pin_source_address_from_view)
update_pin_source_address_from_view.set_downstream(extract_distinct_pin_source_addresses)
extract_distinct_pin_source_addresses.set_downstream(standardize_address_comps)
standardize_address_comps.set_downstream(write_std_pin_source_address_comps)
write_std_pin_source_address_comps.set_downstream(update_pin_source_address_plus_std_from_view)
update_pin_source_address_plus_std_from_view.set_downstream(cleanup)
# update_pin_source_address_plus_std_from_view.set_downstream(extract_pin_source_address_plus_std)
# extract_pin_source_address_plus_std.set_downstream(write_pin_source_address_plus_std)
# write_pin_source_address_plus_std.set_downstream(delete_temp_pin_source_address_plus_std)
# delete_temp_pin_source_address_plus_std.set_downstream(cleanup)

write_tripoli_dor_parcel.set_downstream(update_dor_parcel_pin_changes_geom)
write_tripoli_dor_pin_changes.set_downstream(update_dor_parcel_pin_changes_geom)
update_dor_parcel_pin_changes_geom.set_downstream(cleanup)


extract_tripoli_dor_pin_changes.set_upstream(make_staging)
extract_tripoli_dor_pin_changes.set_downstream(write_tripoli_dor_pin_changes)
write_tripoli_dor_pin_changes.set_downstream(update_tripoli_dor_pin_changes_hash)
write_tripoli_dor_pin_changes.set_downstream(delete_temp_tripoli_dor_pin_changes)
update_tripoli_dor_pin_changes_hash.set_downstream(update_tripoli_dor_pin_changes_history)
update_tripoli_dor_pin_changes_history.set_downstream(cleanup)

extract_tripoli_dor_parcelhistory.set_upstream(make_staging)
extract_tripoli_dor_parcelhistory.set_downstream(write_tripoli_dor_parcelhistory)
write_tripoli_dor_parcelhistory.set_downstream(delete_temp_tripoli_dor_parcelhistory)
delete_temp_tripoli_dor_parcelhistory.set_downstream(cleanup)

#extract_tripoli_parcel_cleanup_pin_queue.set_upstream(make_staging)
#extract_tripoli_parcel_cleanup_pin_queue.set_downstream(write_tripoli_parcel_cleanup_pin_queue)
#write_tripoli_parcel_cleanup_pin_queue.set_downstream(update_tripoli_parcel_cleanup_pin_queue_hash)
#write_tripoli_parcel_cleanup_pin_queue.set_downstream(delete_temp_tripoli_parcel_cleanup_pin_queue)
#update_tripoli_parcel_cleanup_pin_queue_hash.set_downstream(cleanup)

extract_tripoli_parcel_cleanup_missing_entities.set_upstream(make_staging)
extract_tripoli_parcel_cleanup_missing_entities.set_downstream(write_tripoli_parcel_cleanup_missing_entities)
write_tripoli_parcel_cleanup_missing_entities.set_downstream(update_tripoli_parcel_cleanup_missing_entities_hash)
write_tripoli_parcel_cleanup_missing_entities.set_downstream(delete_temp_tripoli_parcel_cleanup_missing_entities)
update_tripoli_parcel_cleanup_missing_entities_hash.set_downstream(cleanup)


write_tripoli_dor_parcel.set_upstream(update_tripoli_dor_parcel_geom_compare)
update_tripoli_dor_parcel_geom_compare.set_downstream(update_tripoli_dor_parcel_geom_changes)
update_tripoli_dor_parcel_geom_changes.set_upstream(write_tripoli_dor_parcel)
update_tripoli_dor_parcel_geom_changes.set_downstream(extract_last_update_from_tripoli_planning_dor_parcel_geom_changes)
extract_last_update_from_tripoli_planning_dor_parcel_geom_changes.set_downstream(extract_dor_parcel_geom_changes_for_planning_for_revenue)
extract_dor_parcel_geom_changes_for_planning_for_revenue.set_downstream(write_dor_parcel_geom_change_updates_for_planning_to_tripoli)
write_dor_parcel_geom_change_updates_for_planning_to_tripoli.set_downstream(cleanup)


upsert_property_parcel.set_upstream(write_tripoli_dor_parcel)
upsert_property_parcel.set_upstream(write_tripoli_dor_pin_changes)
upsert_property_parcel.set_upstream(write_pwd_parcels)
upsert_property_parcel.set_downstream(update_property_parcel_status_for_inactives)
update_property_parcel_status_for_inactives.set_downstream(cleanup)


extract_dor_condominium.set_upstream(make_staging)
extract_dor_condominium.set_downstream(write_dor_condominium)
write_dor_condominium.set_downstream(cleanup)