import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator, GeopetlUpsertOperator
from airflow.hooks import GeopetlHook
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.pin_source_address_std import check_address_comps
from airflow.utils.pin_sql_v2 import *
from datetime import datetime, timedelta #, timezone
from pytz import timezone 
from airflow.models import Variable
import petl as etl

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 15, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_cama_geopetl_v1', schedule_interval='0 23 * * 0,1,2,3,4,5', default_args=default_args)
#pipeline = DAG('etl_cama_geopetl_v1', schedule_interval=None, default_args=default_args)

# -----------------------------------------------------------
def delete_temp_file(**kwargs):
    path = kwargs['templates_dict']['csv_path']
    filename = kwargs['templates_dict']['filename']
    os.remove(path + '/' + filename)


def update_postgres(**kwargs):
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

def update_db(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)

# TODO: change generalize stmt variable names:

def query_oracle(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    print(stmt)
    geopetl_hook = GeopetlHook(db_conn_id=db_conn_id)
    conn = geopetl_hook.get_conn()
    cur = conn.cursor()
    cur.execute(stmt)
    conn.commit()


def insert_audit_record(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt = kwargs['stmt']
    last_update_schema_name=kwargs['last_update_schema_name']
    last_update_table_name=kwargs['last_update_table_name']
    upload_file_name=kwargs['upload_file_name']
    upload_file_date=kwargs['upload_file_date']
    upload_interface=kwargs['upload_interface']
    update_date_file = templates_dict.get('csv_path', '')
    last_update_date = etl.fromcsv(update_date_file)[1][0]
    stmt = stmt.format(last_update_date=last_update_date, last_update_schema_name=last_update_schema_name, last_update_table_name=last_update_table_name, upload_file_name=upload_file_name, upload_file_date=upload_file_date, upload_interface=upload_interface)
    print(stmt)
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)


update_table_from_view_stmt = '''BEGIN;
truncate table {table_name};
insert into {table_name} (select * from {view_name});
COMMIT;
'''

# get last updated timestamp from db oracle revenue interface:
get_last_asmt_update_from_oracle_for_revenue_stmt = '''select max(etl_modified_timestamp) as last_update from {table_name}'''

# get last updated timestamp from audit table:
get_last_update_from_audit_stmt = ''' select max(last_etl_timestamp_processed) as last_update from audit.file_upload_history where schema_name = '{last_update_schema_name}' and table_name = '{last_update_table_name}' '''

# get updates since last updated timestamp from db2:
get_updates_from_db2_for_revenue_stmt = '''
    select {fields} 
    from (
        select distinct on (etl_hash) *
        from {table_name}
        ORDER BY etl_hash, etl_action_timestamp DESC NULLS LAST
    ) foo where etl_action <> 'delete'::text
'''
get_updates_from_db2_for_revenue_stmt_where = ''' and etl_action_timestamp > '{last_update_date}' '''

get_updates_from_db2_since_last_update_from_audit_stmt = '''
select prep.*
from ({prep_stmt}) prep,
({last_update_from_audit_stmt}) last_update
where prep.etl_modified_timestamp > last_update.last_update
'''

insert_audit_file_upload_history_stmt = '''BEGIN;
insert into audit.file_upload_history (schema_name, table_name, upload_file_name, upload_file_date, last_etl_timestamp_processed, upload_interface)
values('{last_update_schema_name}', '{last_update_table_name}', '{upload_file_name}', '{upload_file_date}', '{last_update_date}', '{upload_interface}');
COMMIT;
'''


# SELECT FIELDS
asmt_update_fields = '''tax_year,pin,opa_account_num,street_address,street_code,address_low,address_low_suffix,address_high,unit_num,asmt_mkt_val,asmt_tax_val,asmt_exmpt_val,exempt_code,zip,building_code,mailing_care_of,mailing_street_address,
mailing_address_1,mailing_address_2,mailing_city_state,mailing_zip,category_code,cert_action_date,taxable_land,taxable_building,exempt_land,exempt_building,cert_reason_code,transaction_num,etl_action_timestamp as etl_modified_timestamp, owner_1, owner_2'''

splcom_update_fields = '''oldid AS old_pin,
		newid AS new_pin,
		oldalt_id AS old_opa_account_num,
		newalt_id AS new_opa_account_num,
		taxyr AS tax_year,
		splitno AS splcom_num,
		status,
		splitcde AS xref_code,
		wen AS record_date,
		trans_id AS transaction_num,
                etl_action_timestamp as etl_modified_timestamp'''

db2_splcom_updates_table_name = 'cama.splcom_history'

opa_account_num_history_update_fields = '''parid AS pin,
    taxyr AS tax_year,
    new_alt_id AS new_opa_account_num,
    old_alt_id AS old_opa_account_num,
    trans_id AS transaction_num,
    wen AS record_date,
    etl_action_timestamp as etl_modified_timestamp
'''

db2_opa_account_num_history_updates_table_name = 'cama.phl_acct_hist_det_history'

splcom_assessments_update_fields = '''
assessment_value
,assessment_year
,cert_action_date
,mailing_care_of
,mailing_street_address
,mtm_parent_opa_account_nums
,mtm_parent_pins
,new_opa_account_num
,new_pin
,old_opa_account_num
,old_pin
,owner_1
,owner_2
,owner_type
,parent_asmt_closed
,splcom_num
,street_address
,xref_code,
etl_action_timestamp as etl_modified_timestamp
'''

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from OPA

extract_phl_adrcrtval_vw = GeopetlReadOperator(
    task_id='read_phl_adrcrtval_vw',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_adrcrtval_vw.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.phl_adrcrtval_vw',
    db_table_where='',
)

extract_splcom = GeopetlReadOperator(
    task_id='read_splcom',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/splcom.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.splcom',
    db_table_where='',
)

extract_pardat = GeopetlReadOperator(
    task_id='read_pardat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.pardat',
    db_table_where='',
)

extract_owndat = GeopetlReadOperator(
    task_id='read_owndat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owndat.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.owndat',
    db_table_where='',
)

extract_maildat = GeopetlReadOperator(
    task_id='read_maildat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/maildat.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.maildat',
    db_table_where='',
)

extract_dweldat = GeopetlReadOperator(
    task_id='read_dweldat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dweldat.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.dweldat',
    db_table_where='',
)

extract_asmt_all = GeopetlReadOperator(
    task_id='read_asmt_all',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.asmt_all',
    db_sql='select parid, taxyr, wen, trans_id, tot03, tot08, tot09, tot10, tot11, reascd, seq, current_timestamp as etl_read_timestamp from philly_prod.asmt_all',
)

extract_exdet = GeopetlReadOperator(
    task_id='read_exdet',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/exdet.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.exdet',
    db_sql='select parid, taxyr, excode, begdt, wen, cur, current_timestamp as etl_read_timestamp from philly_prod.exdet',
)

extract_legdat = GeopetlReadOperator(
    task_id='read_legdat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/legdat.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.legdat',
    db_table_where='',
)

extract_sales =  GeopetlReadOperator(
    task_id='read_sales',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/sales.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.sales',
    db_table_where='',
)

extract_phl_asmt =  GeopetlReadOperator(
    task_id='read_phl_asmt',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_asmt.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.phl_asmt',
    db_table_where='',
)

extract_phl_acct_hist_det =  GeopetlReadOperator(
    task_id='read_phl_acct_hist_det',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_acct_hist_det.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.phl_acct_hist_det',
    db_table_where='',
)


extract_land = GeopetlReadOperator(
    task_id='read_land',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/land.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.land',
    db_table_where='',
)

extract_comdat = GeopetlReadOperator(
    task_id='read_comdat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/comdat.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.comdat',
    db_table_where='',
)

extract_comintext = GeopetlReadOperator(
    task_id='read_comintext',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/comintext.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.comintext',
    db_table_where='',
)

extract_oby = GeopetlReadOperator(
    task_id='read_oby',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/oby.csv',
    db_conn_id='cama-prod',
    db_table_name='philly_prod.oby',
    db_table_where='',
)


extract_asmt_cert_updates =  GeopetlReadOperator(
    task_id='read_asmt_cert_updates',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt_cert_updates.csv',
    db_conn_id='databridge2',
    db_table_name='cama.vw_assessment_cert_updates_for_revenue',
    db_table_where='',
)

# extract_homestead_exemptions = GeopetlReadOperator(
#     task_id='read_homestead_exemptions',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/homestead_exemptions.csv',
#     db_conn_id='databridge',
#     db_table_name='gis_revenue.homestead_exemptions',
#     db_table_where='',
# )

# extract_homestead_exemptions = GeopetlReadOperator(
#     task_id='read_homestead_exemptions',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/homestead_exemptions.csv',
#     db_conn_id='databridge-v2-citygeo',
#     db_table_name='viewer_revenue.homestead_exemptions',
#     db_table_where='',
# )

extract_homestead_exemptions_all = GeopetlReadOperator(
    task_id='read_homestead_exemptions_all',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead_exemptions_all.csv',
    db_conn_id='databridge-v2-citygeo',
    db_table_name='viewer_revenue.homestead_exemptions_all',
    db_table_where='',
)

#extract_homestead = GeopetlReadOperator(
#    task_id='read_homestead',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead.csv',
#    db_conn_id='databridge',
#    db_table_name='gis_revenue.homestead',
#    db_table_where='',
#)

#extract_homestead_archive = GeopetlReadOperator(
#    task_id='read_homestead_archive',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead_archive.csv',
#    db_conn_id='databridge',
#    db_table_name='gis_revenue.homestead_archive',
#    db_table_where='',
#)

extract_address_servicearea_summary = GeopetlReadOperator(
    task_id='read_address_servicearea_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/address_servicearea_summary.csv',
    db_conn_id='databridge',
    db_table_name='gis_ais.vw_address_servicearea_summary',
    db_table_where='',
)

extract_property_codes_for_water = GeopetlReadOperator(
    task_id='read_property_codes_for_water',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_codes.csv',
    db_conn_id='databridge2',
    db_table_name='opa.vw_property_codes_for_water',
    db_table_where='',
)

extract_processed_deeds = GeopetlReadOperator(
    task_id='read_processed_deeds',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/processed_deeds.csv',
    db_conn_id='databridge2',
    db_table_name='cama.vw_processed_deeds',
    db_table_where='',
)
# ----------------------------------------------------
# Write extracted files to Databridge

write_phl_adrcrtval_vw = GeopetlWriteOperator(
    task_id='write_phl_adrcrtval_vw',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_adrcrtval_vw.csv',
    db_conn_id='databridge2',
    db_table_name='cama.phl_adrcrtval_vw',
    db_table_where='',
)

write_splcom = GeopetlWriteOperator(
    task_id='write_splcom',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/splcom.csv',
    db_conn_id='databridge2',
    db_table_name='cama.splcom',
    db_table_where='',
)

write_pardat = GeopetlWriteOperator(
    task_id='write_pardat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat.csv',
    db_conn_id='databridge2',
    db_table_name='cama.pardat',
    db_table_where='',
)

write_owndat = GeopetlWriteOperator(
    task_id='write_owndat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owndat.csv',
    db_conn_id='databridge2',
    db_table_name='cama.owndat',
    db_table_where='',
)

write_maildat = GeopetlWriteOperator(
    task_id='write_maildat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/maildat.csv',
    db_conn_id='databridge2',
    db_table_name='cama.maildat',
    db_table_where='',
)

write_dweldat = GeopetlWriteOperator(
    task_id='write_dweldat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dweldat.csv',
    db_conn_id='databridge2',
    db_table_name='cama.dweldat',
    db_table_where='',
)

write_asmt = GeopetlWriteOperator(
    task_id='write_asmt',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt.csv',
    db_conn_id='databridge2',
    db_table_name='cama.asmt',
    db_table_where='',
)

write_exdet = GeopetlWriteOperator(
    task_id='write_exdet',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/exdet.csv',
    db_conn_id='databridge2',
    db_table_name='cama.exdet',
    db_table_where='',
)

write_legdat = GeopetlWriteOperator(
    task_id='write_legdat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/legdat.csv',
    db_conn_id='databridge2',
    db_table_name='cama.legdat',
    db_table_where='',
)

write_sales = GeopetlWriteOperator(
    task_id='write_sales',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/sales.csv',
    db_conn_id='databridge2',
    db_table_name='cama.sales',
    db_table_where='',
)

write_phl_asmt = GeopetlWriteOperator(
    task_id='write_phl_asmt',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_asmt.csv',
    db_conn_id='databridge2',
    db_table_name='cama.phl_asmt',
    db_table_where='',
)

write_phl_acct_hist_det = GeopetlWriteOperator(
    task_id='write_phl_acct_hist_det',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_acct_hist_det.csv',
    db_conn_id='databridge2',
    db_table_name='cama.phl_acct_hist_det',
    db_table_where='',
)

write_land = GeopetlWriteOperator(
    task_id='write_land',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/land.csv',
    db_conn_id='databridge2',
    db_table_name='cama.land',
)

write_comdat = GeopetlWriteOperator(
    task_id='write_comdat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/comdat.csv',
    db_conn_id='databridge2',
    db_table_name='cama.comdat',
)

write_comintext = GeopetlWriteOperator(
    task_id='write_comintext',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/comintext.csv',
    db_conn_id='databridge2',
    db_table_name='cama.comintext',
)

write_oby = GeopetlWriteOperator(
    task_id='write_oby',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/oby.csv',
    db_conn_id='databridge2',
    db_table_name='cama.oby',
)


write_asmt_cert_updates = GeopetlWriteOperator(
    task_id='write_asmt_cert_updates',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt_cert_updates.csv',
    db_conn_id='databridge2',
    db_table_name='opa.assessment_cert_updates',
    db_table_where='',
)

# write_homestead_exemptions = GeopetlWriteOperator(
#     task_id='write_homestead_exemptions',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/homestead_exemptions.csv',
#     db_conn_id='databridge2',
#     db_table_name='revenue.databridge_homestead_exemptions',
#     db_table_where='',
# )

write_homestead_exemptions_all = GeopetlWriteOperator(
    task_id='write_homestead_exemptions_all',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead_exemptions_all.csv',
    db_conn_id='databridge2',
    db_table_name='revenue.databridge_homestead_exemptions_all',
    db_table_where='',
)
#write_homestead = GeopetlWriteOperator(
#    task_id='write_homestead',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead.csv',
#    db_conn_id='databridge2',
#    db_table_name='revenue.databridge_homestead',
#    db_table_where='',
#)

#write_homestead_archive = GeopetlWriteOperator(
#    task_id='write_homestead_archive',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead_archive.csv',
#    db_conn_id='databridge2',
#    db_table_name='revenue.databridge_homestead_archive',
#    db_table_where='',
#)

write_address_servicearea_summary = GeopetlWriteOperator(
    task_id='write_address_servicearea_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/address_servicearea_summary.csv',
    db_conn_id='databridge2',
    db_table_name='ais.databridge_address_servicearea_summary',
)

write_property_codes_for_water = GeopetlWriteOperator(
    task_id='write_property_codes_for_water',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_codes.csv',
    db_conn_id='databridge-opa',
    db_table_name='gis_opa.property_codes',
)

write_processed_deeds = GeopetlWriteOperator(
    task_id='write_processed_deeds',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/processed_deeds.csv',
    db_conn_id='databridge-cama',
    db_table_name='gis_cama.processed_deeds',
)
# -----------------------------------------------------------------
# Cleanup temp files

delete_temp_phl_adrcrtval_vw = PythonOperator(
    task_id='delete_temp_phl_adrcrtval_vw',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'phl_adrcrtval_vw.csv',},
)


delete_temp_splcom = PythonOperator(
    task_id='delete_temp_splcom',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'splcom.csv',},
)

delete_temp_pardat = PythonOperator(
    task_id='delete_temp_pardat',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'pardat.csv',},
)

delete_temp_owndat = PythonOperator(
    task_id='delete_temp_owndat',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'owndat.csv',},
)

delete_temp_maildat = PythonOperator(
    task_id='delete_temp_maildat',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'maildat.csv',},
)

delete_temp_dweldat = PythonOperator(
    task_id='delete_temp_dweldat',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'dweldat.csv',},
)

delete_temp_asmt = PythonOperator(
    task_id='delete_temp_asmt',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'asmt.csv',},
)

delete_temp_exdet = PythonOperator(
    task_id='delete_temp_exdet',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'exdet.csv',},
)

delete_temp_legdat = PythonOperator(
    task_id='delete_temp_legdat',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'legdat.csv',},
)

delete_temp_sales = PythonOperator(
    task_id='delete_temp_sales',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'sales.csv',},
)

delete_temp_phl_asmt = PythonOperator(
    task_id='delete_temp_phl_asmt',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'phl_asmt.csv',},
)

delete_temp_phl_acct_hist_det = PythonOperator(
    task_id='delete_temp_phl_acct_hist_det',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'phl_acct_hist_det.csv',},
)

delete_temp_land = PythonOperator(
    task_id='delete_temp_land',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'land.csv',},
)

delete_temp_comdat = PythonOperator(
    task_id='delete_temp_comdat',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'comdat.csv',},
)

delete_temp_comintext = PythonOperator(
    task_id='delete_temp_comintext',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'comintext.csv',},
)

delete_temp_oby = PythonOperator(
    task_id='delete_temp_oby',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'oby.csv',},
)



delete_temp_asmt_cert_updates = PythonOperator(
    task_id='delete_temp_asmt_cert_updates',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'asmt_cert_updates.csv',},
)

#delete_temp_homestead = PythonOperator(
#    task_id='delete_temp_homestead',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'homestead.csv',},
#)

#delete_temp_homestead_archive = PythonOperator(
#    task_id='delete_temp_homestead_archive',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'homestead_archive.csv',},
#)

delete_temp_address_servicearea_summary = PythonOperator(
    task_id='delete_temp_address_servicearea_summary',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'/address_servicearea_summary.csv',},
)

delete_temp_property_codes_for_water = PythonOperator(
    task_id='delete_temp_property_codes_for_water',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'/property_codes.csv',},
)

# -----------------------------------------------------------------
# Update hashes
#

update_phl_adrcrtval_vw_hash = PythonOperator(
    task_id='update_phl_adrcrtval_v2_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'phl_adrcrtval_vw', 'hash_field': 'etl_hash'},
)


update_splcom_hash = PythonOperator(
    task_id='update_splcom_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'splcom', 'hash_field': 'etl_hash'},
)

update_pardat_hash = PythonOperator(
    task_id='update_pardat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'pardat', 'hash_field': 'etl_hash'},
)

update_owndat_hash = PythonOperator(
    task_id='update_owndat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'owndat', 'hash_field': 'etl_hash'},
)

update_maildat_hash = PythonOperator(
    task_id='update_maildat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'maildat', 'hash_field': 'etl_hash'},
)

update_dweldat_hash = PythonOperator(
    task_id='update_dweldat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'dweldat', 'hash_field': 'etl_hash'},
)

update_asmt_hash = PythonOperator(
    task_id='update_asmt_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'asmt', 'hash_field': 'etl_hash'},
)

update_exdet_hash = PythonOperator(
    task_id='update_exdet_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'exdet', 'hash_field': 'etl_hash'},
)

update_legdat_hash = PythonOperator(
    task_id='update_legdat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'legdat', 'hash_field': 'etl_hash'},
)

update_sales_hash = PythonOperator(
    task_id='update_sales_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_phl_asmt_hash = PythonOperator(
    task_id='update_phl_asmt_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'phl_asmt', 'hash_field': 'etl_hash'},
)

update_phl_acct_hist_det_hash = PythonOperator(
    task_id='update_phl_acct_hist_det_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'phl_acct_hist_det', 'hash_field': 'etl_hash'},
)

update_asmt_cert_updates_hash = PythonOperator(
    task_id='update_asmt_cert_updates_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'assessment_cert_updates', 'hash_field': 'etl_hash'},
)

#update_homestead_hash = PythonOperator(
#    task_id='update_homestead_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'revenue', 'table_name': 'databridge_homestead', 'hash_field': 'etl_hash'},
#)

#update_homestead_archive_hash = PythonOperator(
#    task_id='update_homestead_archive_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'revenue', 'table_name': 'databridge_homestead_archive', 'hash_field': 'etl_hash'},
#)


# -----------------------------------------------------------------
# Update histories
#

update_phl_adrcrtval_vw_history = PythonOperator(
    task_id='update_phl_adrcrtval_vw_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'phl_adrcrtval_vw', 'hash_field': 'etl_hash'},
)


update_splcom_history = PythonOperator(
    task_id='update_splcom_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'splcom', 'hash_field': 'etl_hash'},
)

update_pardat_history = PythonOperator(
    task_id='update_pardat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'pardat', 'hash_field': 'etl_hash'},
)

update_owndat_history = PythonOperator(
    task_id='update_owndat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'owndat', 'hash_field': 'etl_hash'},
)

update_maildat_history = PythonOperator(
    task_id='update_maildat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'maildat', 'hash_field': 'etl_hash'},
)

update_dweldat_history = PythonOperator(
    task_id='update_dweldat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'dweldat', 'hash_field': 'etl_hash'},
)

update_asmt_history = PythonOperator(
    task_id='update_asmt_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'asmt', 'hash_field': 'etl_hash'},
)

update_exdet_history = PythonOperator(
    task_id='update_exdet_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'exdet', 'hash_field': 'etl_hash'},
)

update_legdat_history = PythonOperator(
    task_id='update_legdat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'legdat', 'hash_field': 'etl_hash'},
)

update_sales_history = PythonOperator(
    task_id='update_sales_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_phl_asmt_history = PythonOperator(
    task_id='update_phl_asmt_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'phl_asmt', 'hash_field': 'etl_hash'},
)

update_phl_acct_hist_det_history = PythonOperator(
    task_id='update_phl_acct_hist_det_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama', 'table_name': 'phl_acct_hist_det', 'hash_field': 'etl_hash'},
)

update_asmt_cert_updates_history = PythonOperator(
    task_id='update_asmt_cert_updates_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'assessment_cert_updates', 'hash_field': 'etl_hash'},
)

#update_homestead_history = PythonOperator(
#    task_id='update_homestead_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'revenue', 'table_name': 'databridge_homestead', 'hash_field': 'etl_hash'},
#)

#update_homestead_archive_history = PythonOperator(
#    task_id='update_homestead_archive_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'revenue', 'table_name': 'databridge_homestead_archive', 'hash_field': 'etl_hash'},
#)

#--------------------------------------------------------------------------------------------------------------------------
# Additional tasks:

#----
# New Dev Changes:

property_assessments_unfiltered_table_name = 'opa.property_assessments_unfiltered'
property_assessments_unfiltered_view_name = 'opa.vw_property_assessments_unfiltered'
#property_assessments_unfiltered_view_name = 'opa.vw_property_assessments_unfiltered_dev'
update_property_assessments_unfiltered = PythonOperator(
    task_id='update_property_assessments_unfiltered',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_assessments_unfiltered_table_name,view_name=property_assessments_unfiltered_view_name)},
)

property_assessments_for_summary_unfiltered_table_name = 'opa.property_assessments_for_summary_unfiltered'
property_assessments_for_summary_unfiltered_view_name = 'opa.vw_property_assessments_for_summary_unfiltered'
#property_assessments_for_summary_unfiltered_view_name = 'opa.vw_property_assessments_for_summary_unfiltered_dev'
update_property_assessments_for_summary_unfiltered = PythonOperator(
    task_id='update_property_assessments_for_summary_unfiltered',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_assessments_for_summary_unfiltered_table_name,view_name=property_assessments_for_summary_unfiltered_view_name)},
)

latest_sales_table_name = 'cama.latest_sales'
latest_sales_view_name = 'cama.vw_latest_sales'
update_latest_sales = PythonOperator(
    task_id='update_latest_sales',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=latest_sales_table_name,view_name=latest_sales_view_name)},
)

active_props_table_name = 'cama.active_props'
active_props_view_name = 'cama.vw_active_props'
update_active_props = PythonOperator(
    task_id='update_active_props',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=active_props_table_name,view_name=active_props_view_name)},
)

#-----



extract_last_asmt_update_from_oracle_for_revenue = GeopetlReadOperator(
    task_id='read_last_asmt_update_from_oracle_for_revenue',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_asmt_update_timestamp.csv',
    db_conn_id='databridge-opa',
    db_table_name='assessment_cert_update',
    db_table_where='',
    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='assessment_cert_update')
)

#extract_last_asmt_update_from_oracle_test_for_revenue = GeopetlReadOperator(
#    task_id='read_last_asmt_update_from_oracle_test_for_revenue',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/last_test_asmt_update_timestamp.csv',
#    db_conn_id='gisdbp_t_gis_opa',
#    db_table_name='assessment_cert_update',
#    db_table_where='',
#    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='assessment_cert_update')
#)

db2_asmt_updates_schema_table_name = 'opa.assessment_cert_updates_history'
db2_asmt_updates_schema_name=db2_asmt_updates_schema_table_name.split('.')[0]
db2_asmt_updates_table_name=db2_asmt_updates_schema_table_name.split('.')[1]

extract_asmt_updates_for_revenue = PythonOperator(
    task_id='read_asmt_updates_for_revenue',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_asmt_updates_schema_table_name, 'stmt': get_updates_from_db2_for_revenue_stmt.format(table_name=db2_asmt_updates_schema_table_name, fields=asmt_update_fields),
        'stmt_where': get_updates_from_db2_for_revenue_stmt_where},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/asmt_updates_for_revenue.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_asmt_update_timestamp.csv'},
)

#extract_test_asmt_updates_for_revenue = PythonOperator(
#    task_id='read_test_asmt_updates_for_revenue',
#    dag=pipeline,
#    python_callable=extract_from_postgres,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_asmt_updates_schema_table_name, 'stmt': get_updates_from_db2_for_revenue_stmt.format(table_name=db2_asmt_updates_schema_table_name, fields=asmt_update_fields),
#        'stmt_where': get_updates_from_db2_for_revenue_stmt_where},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/test_asmt_updates_for_revenue.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_test_asmt_update_timestamp.csv'},
#)

write_asmt_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
    task_id='write_asmt_updates_for_revenue',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/asmt_updates_for_revenue.csv',
    db_conn_id='databridge-opa',
    db_table_name='GIS_OPA.ASSESSMENT_CERT_UPDATE',
    db_table_where = '',
    append=True,
)

#extract_asmt_for_sftp_prep_stmt = get_updates_from_db2_for_revenue_stmt.format(table_name=db2_asmt_updates_schema_table_name, fields=asmt_update_fields)
#last_asmt_update_from_audit_stmt=get_last_update_from_audit_stmt.format(last_update_table_name=db2_asmt_updates_table_name, last_update_schema_name=db2_asmt_updates_schema_name)
#extract_asmt_updates_for_revenue_sftp_stmt = get_updates_from_db2_since_last_update_from_audit_stmt.format(prep_stmt=extract_asmt_for_sftp_prep_stmt, last_update_from_audit_stmt=last_asmt_update_from_audit_stmt)

#last_db2_asmt_update_stmt = ''' select max(etl_action_timestamp) as last_update from {last_update_schema_name}.{last_update_table_name} where etl_action != 'delete' '''.format(last_update_schema_name=db2_asmt_updates_schema_name, last_update_table_name=db2_asmt_updates_table_name)
#extract_last_asmt_update_from_db2_for_revenue_audit = GeopetlReadOperator(
#    task_id='read_last_asmt_update_from_db2_for_revenue_audit',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/last_db2_asmt_update_ts_for_audit.csv',
#    db_conn_id='databridge2',
#    db_table_name='cama.asmt_history',
#    db_table_where='',
#    db_sql=last_db2_asmt_update_stmt
#)

#extract_asmt_updates_for_revenue_sftp = PythonOperator(
#    task_id='read_asmt_updates_for_revenue_sftp',
#    dag=pipeline,
#    python_callable=extract_from_postgres,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_asmt_updates_schema_table_name, 'stmt': extract_asmt_updates_for_revenue_sftp_stmt},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/asmt_updates_for_revenue_sftp.csv'},
#)


## localize timestamp
#eastern = timezone('US/Eastern')
#asmt_sftp_upload_timestamptz = datetime.now(eastern)
##asmt_sftp_upload_timestamptz = datetime.now(timezone.utc)
##asmt_sftp_upload_date = str(datetime.date(asmt_sftp_upload_timestamptz))
#asmt_sftp_file_name = 'asmt_updates_' + str(asmt_sftp_upload_timestamptz).replace(' ', '_').replace(':','') + '.csv'
#asmt_sftp_file_path = f'Assessment_Update/{asmt_sftp_file_name}'
#upload_asmt_updates_for_revenue_to_sftp = SFTPOperator(
#    task_id='upload_asmt_updates_for_revenue_to_sftp',
#    ssh_conn_id='sftp-databridge-revenue-prod',
#    dag=pipeline,
#    local_filepath='{{ ti.xcom_pull("make_staging") }}/asmt_updates_for_revenue_sftp.csv',
#    remote_filepath=asmt_sftp_file_path,
#    operation="put",
#    create_intermediate_dirs=False,
#)

#insert_asmt_sftp_update_audit_record =  PythonOperator(
#    task_id='insert_asmt_sftp_update_audit_record',
#    dag=pipeline,
#    python_callable=insert_audit_record,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'databridge2', 'last_update_schema_name': db2_asmt_updates_schema_name, 'last_update_table_name': db2_asmt_updates_table_name,  'upload_file_name': asmt_sftp_file_name, 'upload_file_date': asmt_sftp_upload_timestamptz, 'upload_interface': 'sftp', 'stmt': insert_audit_file_upload_history_stmt},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/last_db2_asmt_update_ts_for_audit.csv'},
#)

delete_temp_asmt_updates = PythonOperator(
    task_id='delete_temp_asmt_updates',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'asmt_updates_for_revenue.csv',},
)

#delete_temp_asmt_sftp_updates = PythonOperator(
#    task_id='delete_temp_asmt_sftp_updates',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'asmt_updates_for_revenue_sftp.csv',},
#)


#write_test_asmt_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
#    task_id='write_test_asmt_updates_for_revenue',
#    dag=pipeline,
#    csv_path = '{{ ti.xcom_pull("make_staging") }}/test_asmt_updates_for_revenue.csv',
#    db_conn_id='gisdbp_t_gis_opa',
#    db_table_name='GIS_OPA.ASSESSMENT_CERT_UPDATE',
#    db_table_where = '',
#    append=True,
#)

#extract_last_splcom_update_from_oracle_test_for_revenue = GeopetlReadOperator(
#    task_id='read_last_splcom_update_from_oracle_test_for_revenue',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/last_splcom_test_update_timestamp.csv',
#    db_conn_id='gisdbp_t_gis_opa',
#    db_table_name='splcom',
#    db_table_where='',
#    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='splcom')
#)

extract_last_splcom_assessments_update_from_oracle_for_revenue = GeopetlReadOperator(
    task_id='read_last_splcom_assessments_update_from_oracle_for_revenue',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_splcom_assessments_update_timestamp.csv',
    db_conn_id='databridge',
    db_table_name='GIS_OPA.SPLCOM_ASSESSMENTS',
    db_table_where='',
    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='gis_opa.splcom_assessments')
)


splcom_assessments_table_name = 'opa.splcom_assessments'
splcom_assessments_view_name = 'opa.vw_splcom_assessments'
update_splcom_assessments = PythonOperator(
    task_id='update_splcom_assessments',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=splcom_assessments_table_name,view_name=splcom_assessments_view_name)},
)


update_splcom_assessments_hash = PythonOperator(
    task_id='update_splcom_assessments_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'splcom_assessments', 'hash_field': 'etl_hash'},
)

update_splcom_assessments_history = PythonOperator(
    task_id='update_splcom_assessments_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'splcom_assessments', 'hash_field': 'etl_hash'},
)

db2_splcom_assessments_updates_schema_table_name='opa.splcom_assessments_history'
db2_splcom_assessments_updates_schema_name=db2_splcom_assessments_updates_schema_table_name.split('.')[0]
db2_splcom_assessments_updates_table_name=db2_splcom_assessments_updates_schema_table_name.split('.')[1]

extract_splcom_assessments_updates_for_revenue = PythonOperator(
    task_id='read_splcom_assessments_updates_for_revenue',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_splcom_assessments_updates_schema_table_name, 'stmt': get_updates_from_db2_for_revenue_stmt.format(table_name=db2_splcom_assessments_updates_schema_table_name, fields=splcom_assessments_update_fields),
        'stmt_where': get_updates_from_db2_for_revenue_stmt_where},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/splcom_assessments_updates_for_revenue.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_splcom_assessments_update_timestamp.csv'},
)

write_splcom_assessments_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
    task_id='write_splcom_assessments_updates_for_revenue',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/splcom_assessments_updates_for_revenue.csv',
    db_conn_id='databridge-opa',
    db_table_name='GIS_OPA.SPLCOM_ASSESSMENTS',
    db_table_where = '',
    append=True,
)

#extract_for_sftp_prep_stmt = get_updates_from_db2_for_revenue_stmt.format(table_name=db2_splcom_assessments_updates_schema_table_name, fields=splcom_assessments_update_fields)
#sftp_where = 'parent_asmt_closed is true and assessment_value is not null'
#last_update_from_audit_stmt=get_last_update_from_audit_stmt.format(last_update_table_name=db2_splcom_assessments_updates_table_name, last_update_schema_name=db2_splcom_assessments_updates_schema_name)
#extract_splcom_assessments_updates_for_revenue_sftp_stmt = get_updates_from_db2_since_last_update_from_audit_stmt.format(prep_stmt=extract_for_sftp_prep_stmt, last_update_from_audit_stmt=last_update_from_audit_stmt)  + ' and ' + sftp_where


#last_db2_splcom_assessments_update_stmt = ''' select max(etl_action_timestamp) as last_update from {last_update_schema_name}.{last_update_table_name} where etl_action != 'delete' '''.format(last_update_schema_name=db2_splcom_assessments_updates_schema_name, last_update_table_name=db2_splcom_assessments_updates_table_name)
#extract_last_splcom_assessments_update_from_db2_for_revenue_audit = GeopetlReadOperator(
#    task_id='read_last_splcom_assessments_update_from_db2_for_revenue_audit',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/last_db2_splcom_assessments_update_ts_for_audit.csv',
#    db_conn_id='databridge2',
#    db_table_name='opa.splcom_assessments_history',
#    db_table_where='',
#    db_sql=last_db2_splcom_assessments_update_stmt
#)

#extract_splcom_assessments_updates_for_revenue_sftp = PythonOperator(
#    task_id='read_splcom_assessments_updates_for_revenue_sftp',
#    dag=pipeline,
#    python_callable=extract_from_postgres,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_splcom_assessments_updates_schema_table_name, 'stmt': extract_splcom_assessments_updates_for_revenue_sftp_stmt},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/splcom_assessments_updates_for_revenue_sftp.csv'},
#)

## localize timestamp
#eastern = timezone('US/Eastern')
#splcom_assessments_sftp_upload_timestamptz = datetime.now(eastern)
##splcom_assessments_sftp_upload_timestamptz = datetime.now(timezone.utc)
##splcom_assessments_sftp_upload_date = str(datetime.date(splcom_assessments_sftp_upload_timestamptz))
#splcom_assessments_sftp_file_name = 'splcom_assessments_' + str(splcom_assessments_sftp_upload_timestamptz).replace(' ', '_').replace(':','') + '.csv'
#splcom_assessments_sftp_file_path = 'Assessment_Split/' + splcom_assessments_sftp_file_name
#upload_splcom_assessments_updates_for_revenue_to_sftp = SFTPOperator(
#    task_id='upload_splcom_assessments_updates_for_revenue_to_sftp',
#    ssh_conn_id='sftp-databridge-revenue-prod',
#    dag=pipeline,
#    local_filepath='{{ ti.xcom_pull("make_staging") }}/splcom_assessments_updates_for_revenue_sftp.csv',
#    remote_filepath=splcom_assessments_sftp_file_path,
#    operation="put",
#    create_intermediate_dirs=False,
#)

#insert_splcom_assessments_sftp_update_audit_record =  PythonOperator(
#    task_id='insert_splcom_assessments_sftp_update_audit_record',
#    dag=pipeline,
#    python_callable=insert_audit_record,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'databridge2', 'last_update_schema_name': db2_splcom_assessments_updates_schema_name, 'last_update_table_name': db2_splcom_assessments_updates_table_name,  'upload_file_name': splcom_assessments_sftp_file_name, 'upload_file_date': splcom_assessments_sftp_upload_timestamptz, 'upload_interface': 'sftp', 'stmt': insert_audit_file_upload_history_stmt},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/last_db2_splcom_assessments_update_ts_for_audit.csv'},
#)


delete_temp_splcom_assessments_updates = PythonOperator(
    task_id='delete_temp_splcom_assessments_updates',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'splcom_assessments_updates_for_revenue.csv',},
)

#delete_temp_splcom_assessments_sftp_updates = PythonOperator(
#    task_id='delete_temp_splcom_assessments_sftp_updates',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'splcom_assessments_updates_for_revenue_sftp.csv',},
#)

# SPLCOM_FOR_REVENUE FOR SFTP:
# update splcom_for_revenue table from view using upsert:
splcom_for_revenue_table_schema_name = 'cama.splcom_for_revenue'
splcom_for_revenue_updates_schema_name=splcom_for_revenue_table_schema_name.split('.')[0]
splcom_for_revenue_updates_table_name=splcom_for_revenue_table_schema_name.split('.')[1]
splcom_for_revenue_view_schema_name = 'cama.vw_splcom_for_revenue'
update_splcom_for_revenue_stmt = update_splcom_for_revenue_sql
update_splcom_for_revenue = PythonOperator(
    task_id='update_splcom_for_revenue',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_splcom_for_revenue_stmt},
)

# get last update ts for audit for splcom_for_revenue table:
last_splcom_for_revenue_update_ts_stmt = f''' select max(etl_modified_timestamp) as last_update from audit.change_history_for_revenue where tabname = '{splcom_for_revenue_updates_table_name}' and schemaname = '{splcom_for_revenue_updates_schema_name}' and lower(operation) != 'delete' '''
extract_last_splcom_for_revenue_update_ts_for_audit = GeopetlReadOperator(
    task_id='read_last_splcom_for_revenue_update_ts_for_audit',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_splcom_for_revenue_update_ts_for_audit.csv',
    db_conn_id='databridge2',
    db_table_name=splcom_for_revenue_table_schema_name,
    db_table_where='',
    db_sql=last_splcom_for_revenue_update_ts_stmt
)

# extract splcom for revenue updates for sftp:
get_splcom_for_revenue_updates_for_sftp_stmt = get_splcom_for_revenue_updates_for_sftp_sql
extract_splcom_for_revenue_updates_for_sftp = PythonOperator(
    task_id='read_splcom_for_revenue_updates_for_sftp',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': splcom_for_revenue_table_schema_name, 'stmt': get_splcom_for_revenue_updates_for_sftp_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/splcom_for_revenue_updates_for_sftp.csv'},
)

# Upload to splcom for revenue updates to sftp:
# localize timestamp
eastern = timezone('US/Eastern')
splcom_for_revenue_sftp_upload_timestamptz = datetime.now(eastern)
splcom_for_revenue_sftp_file_name = 'splcom_for_revenue_' + str(splcom_for_revenue_sftp_upload_timestamptz).replace(' ', '_').replace(':','') + '.csv'
splcom_for_revenue_sftp_file_path = 'Assessment_Split/' + splcom_for_revenue_sftp_file_name
upload_splcom_for_revenue_updates_to_sftp = SFTPOperator(
    task_id='upload_splcom_for_revenue_updates_to_sftp',
    ssh_conn_id='sftp-databridge-revenue-prod',
    dag=pipeline,
    local_filepath='{{ ti.xcom_pull("make_staging") }}/splcom_for_revenue_updates_for_sftp.csv',
    remote_filepath=splcom_for_revenue_sftp_file_path,
    operation="put",
    create_intermediate_dirs=False,
)

# Insert audit record for splcom_for_revenue sftp update:
insert_splcom_for_revenue_sftp_update_audit_record =  PythonOperator(
    task_id='insert_splcom_for_revenue_sftp_update_audit_record',
    dag=pipeline,
    python_callable=insert_audit_record,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'last_update_schema_name': splcom_for_revenue_updates_schema_name, 'last_update_table_name': splcom_for_revenue_updates_table_name,  'upload_file_name': splcom_for_revenue_sftp_file_name,'upload_file_date': splcom_for_revenue_sftp_upload_timestamptz, 'upload_interface': 'sftp', 'stmt': insert_audit_file_upload_history_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/last_splcom_for_revenue_update_ts_for_audit.csv'},
)


# ASSESSMENT_AND_PROPERTY_UPDATES_FOR_REVENUE FOR SFTP:
# update assessment_and_property_updates_for_revenue table from view using upsert:
assessment_and_property_updates_for_revenue_table_schema_name = 'cama.assessment_and_property_updates_for_revenue'
assessment_and_property_updates_for_revenue_updates_schema_name=assessment_and_property_updates_for_revenue_table_schema_name.split('.')[0]
assessment_and_property_updates_for_revenue_updates_table_name=assessment_and_property_updates_for_revenue_table_schema_name.split('.')[1]
assessment_and_property_updates_for_revenue_view_schema_name = 'cama.vw_assessment_and_property_updates_for_revenue'
update_assessment_and_property_updates_for_revenue_stmt = update_assessment_and_property_updates_for_revenue_sql
update_assessment_and_property_updates_for_revenue = PythonOperator(
    task_id='update_assessment_and_property_updates_for_revenue',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_assessment_and_property_updates_for_revenue_stmt},
)

# get last update ts for audit for assessment_and_property_updates_for_revenue table:
last_assessment_and_property_updates_for_revenue_update_ts_stmt = f''' select max(etl_modified_timestamp) as last_update from audit.change_history_for_revenue where tabname = '{assessment_and_property_updates_for_revenue_updates_table_name}' and schemaname = '{assessment_and_property_updates_for_revenue_updates_schema_name}' and lower(operation) != 'delete' '''
extract_last_assessment_and_property_updates_for_revenue_update_ts_for_audit = GeopetlReadOperator(
    task_id='read_last_assessment_and_property_updates_for_revenue_update_ts_for_audit',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_assessment_and_property_updates_for_revenue_update_ts_for_audit.csv',
    db_conn_id='databridge2',
    db_table_name=assessment_and_property_updates_for_revenue_table_schema_name,
    db_table_where='',
    db_sql=last_assessment_and_property_updates_for_revenue_update_ts_stmt
)

# extract asssessment_and_property_updates_for_revenue updates for sftp:
get_assessment_and_property_updates_for_revenue_for_sftp_stmt = get_assessment_and_property_updates_for_revenue_for_sftp_sql
extract_assessment_and_property_updates_for_revenue_updates_for_sftp = PythonOperator(
    task_id='read_assessment_and_property_updates_for_revenue_updates_for_sftp',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': assessment_and_property_updates_for_revenue_table_schema_name, 'stmt': get_assessment_and_property_updates_for_revenue_for_sftp_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/assessment_and_property_updates_for_revenue_updates_for_sftp.csv'},
)

# Upload to asssessment_and_property_updates_for_revenue updates to sftp:
# localize timestamp
eastern = timezone('US/Eastern')
assessment_and_property_updates_for_revenue_sftp_upload_timestamptz = datetime.now(eastern)
assessment_and_property_updates_for_revenue_sftp_file_name = 'assessment_and_property_updates_for_revenue_' + str(assessment_and_property_updates_for_revenue_sftp_upload_timestamptz).replace(' ', '_').replace(':','') + '.csv'
assessment_and_property_updates_for_revenue_sftp_file_path = 'Assessment_Update/' + assessment_and_property_updates_for_revenue_sftp_file_name
upload_assessment_and_property_updates_for_revenue_updates_to_sftp = SFTPOperator(
    task_id='upload_assessment_and_property_updates_for_revenue_updates_to_sftp',
    ssh_conn_id='sftp-databridge-revenue-prod',
    dag=pipeline,
    local_filepath='{{ ti.xcom_pull("make_staging") }}/assessment_and_property_updates_for_revenue_updates_for_sftp.csv',
    remote_filepath=assessment_and_property_updates_for_revenue_sftp_file_path,
    operation="put",
    create_intermediate_dirs=False,
)

# Insert audit record for assessment_and_property_updates_for_revenue sftp update:
insert_assessment_and_property_updates_for_revenue_sftp_update_audit_record =  PythonOperator(
    task_id='insert_assessment_and_property_updates_for_revenue_sftp_update_audit_record',
    dag=pipeline,
    python_callable=insert_audit_record,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'last_update_schema_name': assessment_and_property_updates_for_revenue_updates_schema_name, 'last_update_table_name': assessment_and_property_updates_for_revenue_updates_table_name,  'upload_file_name': assessment_and_property_updates_for_revenue_sftp_file_name,'upload_file_date': assessment_and_property_updates_for_revenue_sftp_upload_timestamptz, 'upload_interface': 'sftp', 'stmt': insert_audit_file_upload_history_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/last_assessment_and_property_updates_for_revenue_update_ts_for_audit.csv'},
)

write_assessment_and_property_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
    task_id='write_assessment_and_property_updates_for_revenue',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}//assessment_and_property_updates_for_revenue_updates_for_sftp.csv',
    db_conn_id='databridge-opa',
    db_table_name='GIS_OPA.ASSESSMENT_PROPERTY_UPDATES',
    db_table_where = '',
    append=True,
)


extract_last_splcom_update_from_oracle_for_revenue = GeopetlReadOperator(
    task_id='read_last_splcom_update_from_oracle_for_revenue',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_splcom_update_timestamp.csv',
    db_conn_id='databridge',
    db_table_name='GIS_OPA.SPLCOM',
    db_table_where='',
    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='gis_opa.splcom')
)

extract_splcom_updates_for_revenue = PythonOperator(
    task_id='read_splcom_updates_for_revenue',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_splcom_updates_table_name, 'stmt': get_updates_from_db2_for_revenue_stmt.format(table_name=db2_splcom_updates_table_name, fields=splcom_update_fields),
        'stmt_where': get_updates_from_db2_for_revenue_stmt_where},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/splcom_updates_for_revenue.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_splcom_update_timestamp.csv'},
)

#write_splcom_updates_for_revenue_to_db_oracle_test = GeopetlWriteOperator(
#    task_id='write_splcom_updates_for_revenue_test',
#    dag=pipeline,
#    csv_path = '{{ ti.xcom_pull("make_staging") }}/splcom_updates_for_revenue.csv',
#    db_conn_id='gisdbp_t_gis_opa',
#    db_table_name='GIS_OPA.SPLCOM',
#    db_table_where = '',
#    append=True,
#)

write_splcom_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
    task_id='write_splcom_updates_for_revenue',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/splcom_updates_for_revenue.csv',
    db_conn_id='databridge-opa',
    db_table_name='GIS_OPA.SPLCOM',
    db_table_where = '',
    append=True,
)

extract_last_opa_account_num_history_update_from_oracle_for_revenue = GeopetlReadOperator(
    task_id='read_last_opa_account_num_hist_update_from_oracle_for_revenue',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_opa_account_num_history_update_timestamp.csv',
    db_conn_id='databridge-opa',
    db_table_name='opa_account_num_history',
    db_table_where='',
    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='opa_account_num_history')
)

extract_opa_account_num_history_updates_for_revenue = PythonOperator(
    task_id='read_opa_account_num_history_updates_for_revenue',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_opa_account_num_history_updates_table_name, 'stmt': get_updates_from_db2_for_revenue_stmt.format(table_name=db2_opa_account_num_history_updates_table_name, fields=opa_account_num_history_update_fields),
        'stmt_where': get_updates_from_db2_for_revenue_stmt_where},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/opa_account_num_history_updates_for_revenue.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_opa_account_num_history_update_timestamp.csv'},
)

write_opa_account_num_history_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
    task_id='write_opa_account_num_history_updates_for_revenue',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/opa_account_num_history_updates_for_revenue.csv',
    db_conn_id='databridge-opa',
    db_table_name='GIS_OPA.OPA_ACCOUNT_NUM_HISTORY',
    db_table_where = '',
    append=True,
)

# update cama.pardat_latest_prop_data from view:
pardat_latest_prop_data_table_name = 'cama.pardat_latest_prop_data'
pardat_latest_prop_data_view_name = 'cama.vw_pardat_latest_prop_data'
update_pardat_latest_prop_data = PythonOperator(
    task_id='update_pardat_latest_prop_data',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=pardat_latest_prop_data_table_name, view_name=pardat_latest_prop_data_view_name)},
)

# update property.party from view:
property_party_table_name = 'property.party'
property_party_view_name = 'property.vw_party_v4'
update_property_party = PythonOperator(
    task_id='update_property_party',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_party_table_name,view_name=property_party_view_name)},
)

# update property assessments from cama & brtprod:
property_assessments_table_name = 'property.property_assessments'
#property_assessments_view_name = 'property.vw_property_assessments_v3'
property_assessments_view_name = 'opa.vw_property_assessments_active'
update_property_assessments = PythonOperator(
    task_id='update_property_assessments',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_assessments_table_name,view_name=property_assessments_view_name)},
)

extract_property_assessments = GeopetlReadOperator(
    task_id='read_property_assessments',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_assessments.csv',
    db_conn_id='databridge2',
    db_table_name='property.property_assessments',
    db_table_where='',
)

write_property_assessments = GeopetlWriteOperator(
    task_id='write_property_assessments',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_assessments.csv',
    db_conn_id='databridge-opa',
    db_table_name='gis_opa.assessments',
)

opa_assessments_schema = Variable.get('schemas') + 'assessments.json'
trigger_carto_assessments_update = CartoUpdateOperator(
    task_id='update_carto_assessments',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_assessments.csv',
    db_conn_id='carto_phl',
    db_table_name='assessments',
    db_schema_json=opa_assessments_schema,
    #db_indexes_fields=['parcel_number',],
    db_select_users=['publicuser', 'tileuser']
)

delete_temp_property_assessments = PythonOperator(
    task_id='delete_temp_property_assessments',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'property_assessments.csv',},
)

# property_summary_table_name='opa.property_summary'
# property_summary_view_name='opa.vw_property_summary'
# update_property_summary = PythonOperator(
#     task_id='update_property_summary',
#     dag=pipeline,
#     python_callable=update_postgres,
#     op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_summary_table_name,view_name=property_summary_view_name)},
# )

extract_property_summary = GeopetlReadOperator(
    task_id='read_property_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_summary.csv',
    db_conn_id='databridge2',
    db_table_name='opa.property_summary_hybrid_trunc',
    db_table_where='',
)

write_property_summary = GeopetlWriteOperator(
    task_id='write_property_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_summary.csv',
    db_conn_id='databridge-opa',
    db_table_name='gis_opa.property_summary',
)

delete_temp_property_summary = PythonOperator(
    task_id='delete_temp_property_summary',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'property_summary.csv',},
)

update_opa_properties_public_stmt = '''CALL SP_REFRESH_OPA_PROPERTIES_PUB()'''
update_opa_properties_public = PythonOperator(
    task_id='update_opa_properties_public',
    dag=pipeline,
    python_callable=query_oracle,
    op_kwargs={'db_conn_id':'databridge-opa', 'stmt': update_opa_properties_public_stmt},
)

# extract_opa_properties_public = GeopetlReadOperator(
#     task_id='read_opa_properties_public',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/opa_properties_public.csv',
#     db_conn_id='databridge',
#     db_table_name='gis_opa.opa_properties_public',
#     db_timestamp=False,
# )

# opa_properties_public_schema = Variable.get('schemas') + 'opa_properties_public.json'
# trigger_carto_opa_properties_public_update = CartoUpdateOperator(
#     task_id='update_carto_opa_properties_public',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/opa_properties_public.csv',
#     db_conn_id='carto_phl',
#     db_table_name='opa_properties_public',
#     db_schema_json=opa_properties_public_schema,
#     #db_indexes_fields=['parcel_number',],
#     db_select_users=['publicuser', 'tileuser']
# )

# delete_temp_opa_properties_public = PythonOperator(
#     task_id='delete_temp_opa_properties_public',
#     dag=pipeline,
#     python_callable=delete_temp_file,
#     provide_context=True,
#     templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'opa_properties_public.csv',},
# )

update_opa_properties_public_pde_stmt = '''CALL SP_REFRESH_OPA_PROPS_PUB_PDE()'''
update_opa_properties_public_pde = PythonOperator(
    task_id='update_opa_properties_public_pde',
    dag=pipeline,
    python_callable=query_oracle,
    op_kwargs={'db_conn_id':'databridge-opa', 'stmt': update_opa_properties_public_pde_stmt},
)

# extract_opa_properties_public_pde = GeopetlReadOperator(
#     task_id='read_opa_properties_public_pde',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/opa_properties_public_pde.csv',
#     db_conn_id='databridge',
#     db_table_name='gis_opa.opa_properties_public_pde',
#     db_timestamp=False,
# )

# opa_properties_public_pde_schema = Variable.get('schemas') + 'opa_properties_public_pde.json'
# trigger_carto_opa_properties_public_pde_update = CartoUpdateOperator(
#     task_id='update_carto_opa_properties_public_pde',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/opa_properties_public_pde.csv',
#     db_conn_id='carto_phl',
#     db_table_name='opa_properties_public_pde',
#     db_schema_json=opa_properties_public_pde_schema,
#     #db_indexes_fields=['parcel_number','pwd_parcel_id'],
#     db_select_users=['publicuser', 'tileuser']
# )

# delete_temp_opa_properties_public_pde = PythonOperator(
#     task_id='delete_temp_opa_properties_public_pde',
#     dag=pipeline,
#     python_callable=delete_temp_file,
#     provide_context=True,
#     templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'opa_properties_public_pde.csv',},
# )

# --------------------
# opa property summary hybrid dev updates to databridge test and carto gsg account:
property_summary_cama_table_name='opa.property_summary_cama'
#property_summary_cama_view_name='opa.vw_property_summary_cama_active_code_transition'
property_summary_cama_view_name='opa.vw_property_summary_cama_active_code_transition'
update_property_summary_cama = PythonOperator(
    task_id='update_property_summary_cama',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_summary_cama_table_name,view_name=property_summary_cama_view_name)},
)

# property_summary_hybrid_table_name='opa.property_summary_hybrid'
# property_summary_hybrid_view_name='opa.vw_property_summary_hybrid_code_transition'
# update_property_summary_hybrid = PythonOperator(
#     task_id='update_property_summary_hybrid',
#     dag=pipeline,
#     python_callable=update_postgres,
#     op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_summary_hybrid_table_name,view_name=property_summary_hybrid_view_name)},
# )

# Update "truncated" version of property_summary_hybrid to match old property_summary schema:
property_summary_hybrid_trunc_table_name='opa.property_summary_hybrid_trunc'
property_summary_hybrid_trunc_view_name='opa.vw_property_summary_new_trunc'
update_property_summary_hybrid_trunc = PythonOperator(
    task_id='update_property_summary_hybrid_trunc',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_summary_hybrid_trunc_table_name,view_name=property_summary_hybrid_trunc_view_name)},
)


extract_property_summary_new = GeopetlReadOperator(
    task_id='read_property_summary_new',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_summary_new.csv',
    db_conn_id='databridge2',
    db_table_name='opa.property_summary_cama',
    db_table_where='',
)

# write_property_summary_hybrid = GeopetlWriteOperator(
#     task_id='write_property_summary_hybrid',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/property_summary_hybrid.csv',
#     db_conn_id='gisdbp_t_gis_opa',
#     db_table_name='gis_opa.property_summary',
# )

write_property_summary_new_prod = GeopetlWriteOperator(
    task_id='write_property_summary_new_prod',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_summary_new.csv',
    db_conn_id='databridge-opa',
    db_table_name='gis_opa.property_summary_new',
)

write_property_summary_new_prod_db2 = GeopetlWriteOperator(
    task_id='write_property_summary_new_prod_db2',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/property_summary_new.csv',
    db_conn_id='databridge-v2-opa',
    db_table_name='opa.property_summary_new',
)

delete_temp_property_summary_new = PythonOperator(
    task_id='delete_temp_property_summary_new',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'property_summary_new.csv',},
)

# update_test_opa_properties_public_stmt = '''CALL SP_REFRESH_OPA_PROPERTIES_PUB()'''
# update_test_opa_properties_public = PythonOperator(
#     task_id='update_test_opa_properties_public',
#     dag=pipeline,
#     python_callable=query_oracle,
#     op_kwargs={'db_conn_id':'gisdbp_t_gis_opa', 'stmt': update_test_opa_properties_public_stmt},
# )

# extract_test_opa_properties_public = GeopetlReadOperator(
#     task_id='read_test_opa_properties_public',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/test_opa_properties_public.csv',
#     db_conn_id='gisdbp_t',
#     db_table_name='gis_opa.opa_properties_public',
#     db_timestamp=False,
# )

# test_opa_properties_public_schema = Variable.get('schemas') + 'test_opa_properties_public.json'
# trigger_carto_test_opa_properties_public_update = CartoUpdateOperator(
#     task_id='update_carto_test_opa_properties_public',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/test_opa_properties_public.csv',
#     db_conn_id='carto_gsg',
#     db_table_name='opa_properties_public',
#     db_schema_json=test_opa_properties_public_schema,
#     db_select_users=['publicuser', 'tileuser']
# )

# delete_temp_test_opa_properties_public = PythonOperator(
#     task_id='delete_temp_test_opa_properties_public',
#     dag=pipeline,
#     python_callable=delete_temp_file,
#     provide_context=True,
#     templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'test_opa_properties_public.csv',},
# )

# update_test_opa_properties_public_pde_stmt = '''CALL SP_REFRESH_OPA_PROPS_PUB_PDE()'''
# update_test_opa_properties_public_pde = PythonOperator(
#     task_id='update_test_opa_properties_public_pde',
#     dag=pipeline,
#     python_callable=query_oracle,
#     op_kwargs={'db_conn_id':'gisdbp_t_gis_opa', 'stmt': update_test_opa_properties_public_pde_stmt},
# )

# extract_test_opa_properties_public_pde = GeopetlReadOperator(
#     task_id='read_test_opa_properties_public_pde',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/test_opa_properties_public_pde.csv',
#     db_conn_id='gisdbp_t',
#     db_table_name='gis_opa.opa_properties_public_pde',
#     db_timestamp=False,
# )

# test_opa_properties_public_pde_schema = Variable.get('schemas') + 'test_opa_properties_public_pde.json'
# trigger_carto_test_opa_properties_public_pde_update = CartoUpdateOperator(
#     task_id='update_carto_test_opa_properties_public_pde',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/test_opa_properties_public_pde.csv',
#     db_conn_id='carto_gsg',
#     db_table_name='opa_properties_public_pde',
#     db_schema_json=test_opa_properties_public_pde_schema,
#     db_select_users=['publicuser', 'tileuser']
# )

# delete_temp_test_opa_properties_public_pde = PythonOperator(
#     task_id='delete_temp_test_opa_properties_public_pde',
#     dag=pipeline,
#     python_callable=delete_temp_file,
#     provide_context=True,
#     templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'test_opa_properties_public_pde.csv',},
# )

# pin_owner_mailing update:
pin_owner_mailing_table_name='property.pin_owner_mailing'
pin_owner_mailing_view_name='property.vw_pin_owner_mailing'
update_pin_owner_mailing = PythonOperator(
    task_id='update_pin_owner_mailing',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=pin_owner_mailing_table_name,view_name=pin_owner_mailing_view_name)},
)

extract_pin_owner_mailing = GeopetlReadOperator(
    task_id='read_pin_owner_mailing',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pin_owner_mailing.csv',
    db_conn_id='databridge2',
    db_table_name=pin_owner_mailing_table_name,
    db_table_where='',
)

write_pin_owner_mailing = GeopetlWriteOperator(
    task_id='write_pin_owner_mailing',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pin_owner_mailing.csv',
    db_conn_id='databridge-gsg',
    db_table_name='gis_gsg.pin_owner_mailing',
    db_table_where='',
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
    python_callable=update_postgres,
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

# -----------------------------------------------------------------
# Estract and perform standardization report for distinct street_addresses from pin_source_addresses
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
    csv_path='{{ ti.xcom_pull("make_staging") }}/distinct_pin_source_addresses.csv',
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
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}','infile_name': 'distinct_pin_source_addresses.csv', 'outfile_suffix':'_std'},
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
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/distinct_pin_source_addresses_std.csv'},
)

pin_source_address_plus_std_table_name = 'property.pin_source_address_plus_std'
pin_source_address_plus_std_view_name = 'property.vw_pin_source_address_plus_std'
pin_source_address_plus_std_temp_table_name = 'temp_pin_source_address_plus_std'
upsert_pin_source_address_plus_std_stmt = upsert_pin_source_address_plus_std_sql.format(pin_source_address_plus_std_table_schema_name=pin_source_address_plus_std_table_name, pin_source_address_plus_std_view_schema_name=pin_source_address_plus_std_view_name)
update_pin_source_address_plus_std_stmt = '''
BEGIN;
create temp table {pin_source_address_plus_std_temp_table_name} as select * from {pin_source_address_plus_std_view_name};
{upsert_stmt};
--delete from {pin_source_address_plus_std_table_name} main where (pin, address_source) in (
--       select pin, address_source from {pin_source_address_plus_std_table_name}
--        except
--        select pin, address_source from {pin_source_address_plus_std_temp_table_name}
--);
COMMIT;
'''.format(pin_source_address_plus_std_table_name=pin_source_address_plus_std_table_name, pin_source_address_plus_std_view_name=pin_source_address_plus_std_view_name, pin_source_address_plus_std_temp_table_name=pin_source_address_plus_std_temp_table_name, upsert_stmt=upsert_pin_source_address_plus_std_stmt)

# update pin_source_address_plus_std table from view using upsert:
update_pin_source_address_plus_std_from_view = PythonOperator(
    task_id='update_pin_source_address_plus_std_from_view',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_pin_source_address_plus_std_stmt},
)

# extract updated table:
extract_pin_source_address_plus_std = GeopetlReadOperator(
    task_id='read_pin_source_address_plus_std',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pin_source_address_plus_std.csv',
    db_conn_id='databridge2',
    db_table_name='property.pin_source_address_plus_std',
    db_table_where='',
)

write_pin_source_address_plus_std = GeopetlWriteOperator(
    task_id='write_pin_source_address_plus_std',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/pin_source_address_plus_std.csv',
    db_conn_id='databridge-gsg',
    db_table_name='GIS_GSG.PIN_SOURCE_ADDRESS_STD',
    db_table_where = '',
    append=False,
)

delete_temp_pin_source_address_plus_std = PythonOperator(
    task_id='delete_temp_pin_source_address_plus_std',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'pin_source_address_plus_std.csv',},
)


# opa owners cama for ais:
extract_opa_owners_cama_for_ais = GeopetlReadOperator(
    task_id='read_opa_owners_cama_for_ais',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/opa_owners_cama_for_ais.csv',
    db_conn_id='databridge2',
    db_table_name='opa.vw_opa_owners_cama_for_ais',
    db_table_where='',
)

write_opa_owners_cama_for_ais = GeopetlWriteOperator(
    task_id='write_opa_owners_cama_for_ais',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}//opa_owners_cama_for_ais.csv',
    db_conn_id='databridge-ais-sources',
    db_table_name='GIS_AIS_SOURCES.OPA_OWNERS_CAMA_AIS',
    db_table_where = '',
    append=False,
)

delete_temp_opa_owners_cama_for_ais = PythonOperator(
    task_id='delete_temp_opa_owners_cama_for_ais',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'opa_owners_cama_for_ais.csv',},
)

#------------------------------------------------------------------
# Deeds exchange:

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

update_er_stage_transaction_q9_review = PythonOperator(
    task_id='update_er_stage_transaction_q9_review',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_transaction_q9_review_stmt},
)

update_er_stage_parcel_q9_resolution_overridden = PythonOperator(
    task_id='update_er_stage_parcel_q9_resolution_overriden',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_stage_parcel_q9_resolution_overridden_stmt},
)

insert_overridden_records_into_er_stage_parcel_overridden = PythonOperator(
    task_id='insert_overridden_records_into_er_stage_parcel_overridden',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':insert_overridden_records_into_er_stage_parcel_overridden_stmt},
)

delete_overridden_records_from_er_stage_parcel = PythonOperator(
    task_id='delete_overridden_records_from_er_stage_parcel',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':delete_overridden_records_from_er_stage_parcel_stmt},
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

extract_dor_condo_parcel_pin = GeopetlReadOperator(
    task_id='read_dor_condo_parcel_pin',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dor_condo_parcel_pin.csv',
    db_conn_id='databridge2',
    db_table_name='dor.vw_condo_parcel_pin',
    db_table_where='',
)

write_dor_condo_parcel_pin_to_tripoli = GeopetlWriteOperator(
    task_id='write_dor_condo_parcel_pin_to_tripoli',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dor_condo_parcel_pin.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.condo_parcel_pin',
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
sshhook_old_instance = SSHHook(remote_host="citygeo-SC3-aws.city.phila.local",
                                       username="gisscripts",
                                       password=gisscripts.password,
                                       )

sshhook_instance = SSHHook(remote_host="citygeo-SC3-aws.city.phila.local",
                                        username="gisscripts",
                                        password=gisscripts.password,
                                        )

# reverse sync AGO queue 9 table from AGO to Databridge:

backwards_oracle_sync = SSHOperator(
                task_id="backwards_oracle_sync_AGO_queue_9_to_Databridge",
                dag=pipeline,
                command='C:/arcpy/python.exe C:/scripts/ago_to_databridge_backup_etl/ago_to_databridge_backup.py -ad PROPERTY_DEEDS_NEW_QUEUE_9 -d PROPERTY_DEEDS_NEW_QUEUE_9_AGO -a gis_cama --databridge-version 1',
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
#E:/arcpy/python.exe E:/Scripts/ago_update_mulctithread/ago_update.py -d GIS_CAMA_property_deeds_new_queue_9 -o ago -p opa_suspense_queue_review_perms --republish --enable-editing --preserve-editor-tracking
#C:/arcpy/python.exe C:/scripts/ago_updater/ago_update.py -d GIS_CAMA_property_deeds_new_queue_9 -o ago -p opa_suspense_queue_review_perms --republish --enable-editing --preserve-editor-tracking
refresh_ago = SSHOperator(
                task_id="refresh_ago",
                dag=pipeline,
                command="C:/arcpy/python.exe C:/scripts/ago_updater/ago_update.py -d GIS_CAMA_property_deeds_new_queue_9  -o ago -p opa_suspense_queue_review_perms --republish --enable-editing --preserve-editor-tracking",
                ssh_hook=sshhook_old_instance,
                )

backwards_q9_review_findings_sync = SSHOperator(
                task_id="backwards_queue_9_review_findings_AGO_to_Databridge",
                dag=pipeline,
                command="C:/arcpy/python.exe C:/scripts/ago_to_databridge_backup_etl/ago_to_databridge_backup.py -ad '\"OPA Suspense Queue Findings\"' -d QUEUE_9_REVIEW_FINDINGS -a gis_cama --databridge-version 1",
                ssh_hook=sshhook_instance,
                )

# Extract for db2
extract_q9_review_findings = GeopetlReadOperator(
    task_id='extract_q9_review_findings',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/queue_9_review_findings.csv',
    db_conn_id='databridge',
    db_table_name='gis_cama.queue_9_review_findings',
    db_table_where='',
)

# Write to db2
write_q9_review_findings = GeopetlWriteOperator(
    task_id='write_q9_review_findings',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/queue_9_review_findings.csv',
    db_conn_id='databridge2',
    db_table_name='cama.queue_9_review_findings',
    db_table_where='',
)





# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

#make_staging >> extract_splcom >> write_to_databridge >> cleanup
#
extract_phl_adrcrtval_vw.set_upstream(make_staging)
extract_splcom.set_upstream(make_staging)
extract_pardat.set_upstream(make_staging)
extract_owndat.set_upstream(make_staging)
extract_maildat.set_upstream(make_staging)
extract_dweldat.set_upstream(make_staging)
extract_asmt_all.set_upstream(make_staging)
# remove after replace views to read from exdet
#extract_exadmn.set_upstream(make_staging)
extract_exdet.set_upstream(make_staging)
extract_legdat.set_upstream(make_staging)
#extract_comnt.set_upstream(make_staging)
extract_sales.set_upstream(make_staging)
extract_phl_asmt.set_upstream(make_staging)
extract_phl_acct_hist_det.set_upstream(make_staging)
extract_land.set_upstream(make_staging)
extract_comdat.set_upstream(make_staging)
extract_comintext.set_upstream(make_staging)
extract_oby.set_upstream(make_staging)
extract_address_servicearea_summary.set_upstream(make_staging)
# extract_homestead_exemptions.set_upstream(make_staging)
# extract_homestead_exemptions.set_downstream(write_homestead_exemptions)
extract_homestead_exemptions_all.set_upstream(make_staging)
extract_homestead_exemptions_all.set_downstream(write_homestead_exemptions_all)
#extract_homestead.set_upstream(make_staging)
#extract_homestead_archive.set_upstream(make_staging)

extract_phl_adrcrtval_vw.set_downstream(write_phl_adrcrtval_vw)
extract_splcom.set_downstream(write_splcom)
write_splcom.set_downstream(update_splcom_for_revenue)
extract_last_splcom_for_revenue_update_ts_for_audit.set_downstream(insert_splcom_for_revenue_sftp_update_audit_record)
update_splcom_for_revenue.set_downstream(extract_splcom_for_revenue_updates_for_sftp)
extract_splcom_for_revenue_updates_for_sftp.set_downstream(upload_splcom_for_revenue_updates_to_sftp)
extract_splcom_for_revenue_updates_for_sftp.set_downstream(extract_last_splcom_for_revenue_update_ts_for_audit)
upload_splcom_for_revenue_updates_to_sftp.set_downstream(insert_splcom_for_revenue_sftp_update_audit_record)
insert_splcom_for_revenue_sftp_update_audit_record.set_downstream(cleanup)

update_property_assessments_unfiltered.set_downstream(update_splcom_assessments)
update_property_assessments_unfiltered.set_downstream(update_property_assessments)
update_property_assessments_unfiltered.set_downstream(update_property_summary_cama)
update_property_assessments_unfiltered.set_upstream(write_phl_acct_hist_det)
#update_property_assessments_unfiltered.set_upstream(write_homestead)
#update_property_assessments_unfiltered.set_upstream(write_homestead_archive)
# update_property_assessments_unfiltered.set_upstream(write_homestead_exemptions)
update_property_assessments_unfiltered.set_upstream(write_homestead_exemptions_all)
update_property_assessments_unfiltered.set_upstream(write_asmt_cert_updates)
write_splcom.set_downstream(update_splcom_assessments)
update_splcom_assessments.set_upstream(write_splcom)
update_splcom_assessments.set_upstream(update_property_party)
update_splcom_assessments.set_upstream(write_asmt_cert_updates)
update_splcom_assessments.set_downstream(update_active_props)
update_active_props.set_downstream(update_property_assessments)
update_active_props.set_downstream(update_property_summary_cama)
update_active_props.set_upstream(update_pardat_latest_prop_data)
update_active_props.set_upstream(write_sales)
update_active_props.set_upstream(update_property_assessments_unfiltered)
update_active_props.set_upstream(write_phl_acct_hist_det)

update_assessment_and_property_updates_for_revenue.set_upstream(write_pardat)
update_assessment_and_property_updates_for_revenue.set_upstream(update_active_props)
update_assessment_and_property_updates_for_revenue.set_upstream(write_phl_asmt)
update_assessment_and_property_updates_for_revenue.set_upstream(write_exdet)
update_assessment_and_property_updates_for_revenue.set_upstream(write_owndat)
update_assessment_and_property_updates_for_revenue.set_upstream(write_comdat)
update_assessment_and_property_updates_for_revenue.set_upstream(write_dweldat)
update_assessment_and_property_updates_for_revenue.set_downstream(extract_assessment_and_property_updates_for_revenue_updates_for_sftp)
extract_assessment_and_property_updates_for_revenue_updates_for_sftp.set_downstream(upload_assessment_and_property_updates_for_revenue_updates_to_sftp)
extract_assessment_and_property_updates_for_revenue_updates_for_sftp.set_downstream(write_assessment_and_property_updates_for_revenue_to_db_oracle)
write_assessment_and_property_updates_for_revenue_to_db_oracle.set_downstream(cleanup)
extract_assessment_and_property_updates_for_revenue_updates_for_sftp.set_downstream(extract_last_assessment_and_property_updates_for_revenue_update_ts_for_audit)
extract_last_assessment_and_property_updates_for_revenue_update_ts_for_audit.set_downstream(insert_assessment_and_property_updates_for_revenue_sftp_update_audit_record)
upload_assessment_and_property_updates_for_revenue_updates_to_sftp.set_downstream(insert_assessment_and_property_updates_for_revenue_sftp_update_audit_record)
insert_assessment_and_property_updates_for_revenue_sftp_update_audit_record.set_downstream(cleanup)

update_property_assessments_for_summary_unfiltered.set_upstream(write_phl_acct_hist_det)
# update_property_assessments_for_summary_unfiltered.set_upstream(write_homestead_exemptions)
update_property_assessments_for_summary_unfiltered.set_upstream(write_homestead_exemptions_all)

#update_property_assessments_for_summary_unfiltered.set_upstream(write_homestead)
#update_property_assessments_for_summary_unfiltered.set_upstream(write_homestead_archive)
update_property_assessments_for_summary_unfiltered.set_upstream(write_asmt_cert_updates)
update_property_assessments_for_summary_unfiltered.set_downstream(update_property_summary_cama)



extract_pardat.set_downstream(write_pardat)
extract_owndat.set_downstream(write_owndat)
extract_maildat.set_downstream(write_maildat)
extract_dweldat.set_downstream(write_dweldat)
extract_asmt_all.set_downstream(write_asmt)
# remove after replace views to read from exdet
#extract_exadmn.set_downstream(write_exadmn)
extract_exdet.set_downstream(write_exdet)
extract_legdat.set_downstream(write_legdat)
#extract_comnt.set_downstream(write_comnt)
extract_sales.set_downstream(write_sales)
extract_phl_asmt.set_downstream(write_phl_asmt)
extract_phl_acct_hist_det.set_downstream(write_phl_acct_hist_det)
extract_land.set_downstream(write_land)
extract_comdat.set_downstream(write_comdat)
extract_comintext.set_downstream(write_comintext)
extract_oby.set_downstream(write_oby)

#extract_homestead.set_downstream(write_homestead)
#extract_homestead_archive.set_downstream(write_homestead_archive)
extract_asmt_cert_updates.set_upstream(write_phl_asmt)
extract_asmt_cert_updates.set_upstream(write_dweldat)
extract_asmt_cert_updates.set_upstream(write_pardat)
extract_asmt_cert_updates.set_upstream(write_exdet)
extract_asmt_cert_updates.set_upstream(write_owndat)
extract_asmt_cert_updates.set_downstream(write_asmt_cert_updates)
extract_address_servicearea_summary.set_downstream(write_address_servicearea_summary)
write_address_servicearea_summary.set_downstream(delete_temp_address_servicearea_summary)
write_phl_adrcrtval_vw.set_downstream(update_phl_adrcrtval_vw_hash)
write_phl_adrcrtval_vw.set_downstream(delete_temp_phl_adrcrtval_vw)
write_splcom.set_downstream(update_splcom_hash)
write_splcom.set_downstream(delete_temp_splcom)
write_pardat.set_downstream(update_pardat_hash)
write_pardat.set_downstream(delete_temp_pardat)
write_pardat.set_downstream(update_pardat_latest_prop_data)
update_pardat_latest_prop_data.set_downstream(update_property_summary_cama)
update_property_summary_cama.set_upstream(write_address_servicearea_summary)
write_address_servicearea_summary.set_downstream(cleanup)
write_owndat.set_downstream(update_owndat_hash)
write_owndat.set_downstream(delete_temp_owndat)
write_maildat.set_downstream(update_maildat_hash)
write_maildat.set_downstream(delete_temp_maildat)
write_dweldat.set_downstream(update_dweldat_hash)
write_dweldat.set_downstream(delete_temp_dweldat)
write_asmt.set_downstream(update_asmt_hash)
write_asmt.set_downstream(delete_temp_asmt)
# remove after replace views to read from exdet
#write_exadmn.set_downstream(update_exadmn_hash)
#write_exadmn.set_downstream(delete_temp_exadmn)
write_exdet.set_downstream(update_exdet_hash)
write_exdet.set_downstream(delete_temp_exdet)
write_legdat.set_downstream(update_legdat_hash)
write_legdat.set_downstream(delete_temp_legdat)
#write_comnt.set_downstream(update_comnt_hash)
#write_comnt.set_downstream(delete_temp_comnt)
write_sales.set_downstream(update_sales_hash)
write_sales.set_downstream(delete_temp_sales)
write_sales.set_downstream(update_latest_sales)
update_latest_sales.set_downstream(update_property_summary_cama)
write_phl_asmt.set_downstream(update_phl_asmt_hash)
write_phl_asmt.set_downstream(delete_temp_phl_asmt)
write_phl_acct_hist_det.set_downstream(update_phl_acct_hist_det_hash)
write_phl_acct_hist_det.set_downstream(delete_temp_phl_acct_hist_det)
write_land.set_downstream(delete_temp_land)
write_comdat.set_downstream(delete_temp_comdat)
write_comintext.set_downstream(delete_temp_comintext)
write_oby.set_downstream(delete_temp_oby)

write_asmt_cert_updates.set_downstream(update_asmt_cert_updates_hash)
write_asmt_cert_updates.set_downstream(delete_temp_asmt_cert_updates)
write_asmt_cert_updates.set_downstream(update_property_assessments)
update_property_assessments.set_downstream(extract_property_assessments)
extract_property_assessments.set_downstream(write_property_assessments)
extract_property_assessments.set_downstream(trigger_carto_assessments_update)
trigger_carto_assessments_update.set_downstream(cleanup)
write_property_assessments.set_downstream(delete_temp_property_assessments)
trigger_carto_assessments_update.set_downstream(delete_temp_property_assessments)
write_property_assessments.set_downstream(cleanup)

update_property_assessments.set_upstream(update_splcom_assessments)
update_splcom_assessments.set_downstream(update_splcom_assessments_hash)
update_splcom_assessments_hash.set_downstream(update_splcom_assessments_history)
update_splcom_assessments_history.set_downstream(extract_splcom_assessments_updates_for_revenue)

#update_splcom_assessments_history.set_downstream(extract_splcom_assessments_updates_for_revenue_sftp)
#extract_splcom_assessments_updates_for_revenue_sftp.set_downstream(extract_last_splcom_assessments_update_from_db2_for_revenue_audit)
#extract_last_splcom_assessments_update_from_db2_for_revenue_audit.set_downstream(insert_splcom_assessments_sftp_update_audit_record)
#extract_splcom_assessments_updates_for_revenue_sftp.set_downstream(upload_splcom_assessments_updates_for_revenue_to_sftp)
#upload_splcom_assessments_updates_for_revenue_to_sftp.set_downstream(insert_splcom_assessments_sftp_update_audit_record)
#insert_splcom_assessments_sftp_update_audit_record.set_downstream(delete_temp_splcom_assessments_sftp_updates)
#insert_splcom_assessments_sftp_update_audit_record.set_downstream(cleanup)
#upload_splcom_assessments_updates_for_revenue_to_sftp.set_downstream(delete_temp_splcom_assessments_sftp_updates)
#upload_splcom_assessments_updates_for_revenue_to_sftp.set_downstream(cleanup)


extract_splcom_assessments_updates_for_revenue.set_downstream(write_splcom_assessments_updates_for_revenue_to_db_oracle)
write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(delete_temp_splcom_assessments_updates)
write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(cleanup)
extract_last_splcom_assessments_update_from_oracle_for_revenue.set_upstream(make_staging)
extract_last_splcom_assessments_update_from_oracle_for_revenue.set_downstream(extract_splcom_assessments_updates_for_revenue)


#write_homestead.set_downstream(update_homestead_hash)
#write_homestead.set_downstream(delete_temp_homestead)
#write_homestead.set_downstream(update_property_assessments)
#write_homestead_archive.set_downstream(update_homestead_archive_hash)
#write_homestead_archive.set_downstream(delete_temp_homestead_archive)
#write_homestead_archive.set_downstream(update_property_assessments)
update_phl_adrcrtval_vw_hash.set_downstream(update_phl_adrcrtval_vw_history)
update_splcom_hash.set_downstream(update_splcom_history)
update_pardat_hash.set_downstream(update_pardat_history)
update_owndat_hash.set_downstream(update_owndat_history)
update_maildat_hash.set_downstream(update_maildat_history)
update_dweldat_hash.set_downstream(update_dweldat_history)
update_asmt_hash.set_downstream(update_asmt_history)
# remove after replace views to read from exdet
#update_exadmn_hash.set_downstream(update_exadmn_history)
update_exdet_hash.set_downstream(update_exdet_history)
update_legdat_hash.set_downstream(update_legdat_history)
#update_comnt_hash.set_downstream(update_comnt_history)
update_sales_hash.set_downstream(update_sales_history)
update_phl_asmt_hash.set_downstream(update_phl_asmt_history)
update_phl_acct_hist_det_hash.set_downstream(update_phl_acct_hist_det_history)
update_asmt_cert_updates_hash.set_downstream(update_asmt_cert_updates_history)
#update_homestead_hash.set_downstream(update_homestead_history)
#update_homestead_archive_hash.set_downstream(update_homestead_archive_history)
update_phl_adrcrtval_vw_history.set_downstream(cleanup)
update_splcom_history.set_downstream(cleanup)
update_pardat_history.set_downstream(cleanup)
update_owndat_history.set_downstream(cleanup)
update_maildat_history.set_downstream(cleanup)
update_dweldat_history.set_downstream(cleanup)
update_asmt_history.set_downstream(cleanup)
# remove after replace views to read from exdet
#update_exadmn_history.set_downstream(cleanup)
update_exdet_history.set_downstream(cleanup)
update_legdat_history.set_downstream(cleanup)
#update_comnt_history.set_downstream(cleanup)
update_sales_history.set_downstream(cleanup)
update_phl_asmt_history.set_downstream(cleanup)
update_phl_acct_hist_det_history.set_downstream(extract_last_opa_account_num_history_update_from_oracle_for_revenue)
#update_homestead_history.set_downstream(cleanup)
#update_homestead_archive_history.set_downstream(cleanup)

#update_asmt_cert_updates_history.set_downstream(extract_last_asmt_update_from_oracle_for_revenue)
#update_asmt_cert_updates_history.set_downstream(extract_asmt_updates_for_revenue_sftp)
#extract_asmt_updates_for_revenue_sftp.set_downstream(extract_last_asmt_update_from_db2_for_revenue_audit)
#extract_last_asmt_update_from_db2_for_revenue_audit.set_downstream(insert_asmt_sftp_update_audit_record)
#extract_asmt_updates_for_revenue_sftp.set_downstream(upload_asmt_updates_for_revenue_to_sftp)
#upload_asmt_updates_for_revenue_to_sftp.set_downstream(insert_asmt_sftp_update_audit_record)
#insert_asmt_sftp_update_audit_record.set_downstream(delete_temp_asmt_sftp_updates)
#insert_asmt_sftp_update_audit_record.set_downstream(cleanup)
#upload_asmt_updates_for_revenue_to_sftp.set_downstream(delete_temp_asmt_sftp_updates)
#upload_asmt_updates_for_revenue_to_sftp.set_downstream(cleanup)

extract_last_asmt_update_from_oracle_for_revenue.set_upstream(make_staging)
update_asmt_cert_updates_history.set_downstream(extract_asmt_updates_for_revenue)
extract_last_asmt_update_from_oracle_for_revenue.set_downstream(extract_asmt_updates_for_revenue)
extract_asmt_updates_for_revenue.set_downstream(write_asmt_updates_for_revenue_to_db_oracle)
write_asmt_updates_for_revenue_to_db_oracle.set_downstream(cleanup)
write_asmt_updates_for_revenue_to_db_oracle.set_downstream(delete_temp_asmt_updates)
#update_asmt_cert_updates_history.set_downstream(extract_last_asmt_update_from_oracle_test_for_revenue)
#extract_last_asmt_update_from_oracle_test_for_revenue.set_downstream(extract_test_asmt_updates_for_revenue)
#extract_test_asmt_updates_for_revenue.set_downstream(write_test_asmt_updates_for_revenue_to_db_oracle)
#write_test_asmt_updates_for_revenue_to_db_oracle.set_downstream(cleanup)

extract_last_splcom_update_from_oracle_for_revenue.set_upstream(make_staging)
extract_last_splcom_update_from_oracle_for_revenue.set_downstream(extract_splcom_updates_for_revenue)
#extract_splcom_updates_for_revenue.set_downstream(write_splcom_updates_for_revenue_to_db_oracle_test)
write_splcom.set_downstream(extract_splcom_updates_for_revenue)
extract_splcom_updates_for_revenue.set_downstream(write_splcom_updates_for_revenue_to_db_oracle)
#write_splcom_updates_for_revenue_to_db_oracle_test.set_downstream(cleanup)
write_splcom_updates_for_revenue_to_db_oracle.set_downstream(cleanup)
extract_last_opa_account_num_history_update_from_oracle_for_revenue.set_downstream(extract_opa_account_num_history_updates_for_revenue)
extract_opa_account_num_history_updates_for_revenue.set_downstream(write_opa_account_num_history_updates_for_revenue_to_db_oracle)
write_opa_account_num_history_updates_for_revenue_to_db_oracle.set_downstream(cleanup)

update_property_party.set_upstream(update_pardat_latest_prop_data)
update_property_party.set_upstream(write_owndat)
update_property_party.set_downstream(cleanup)

update_property_summary_cama.set_upstream(write_address_servicearea_summary)
update_property_summary_cama.set_upstream(update_property_assessments_for_summary_unfiltered)
# update_property_summary_cama.set_downstream(update_property_summary_hybrid)
update_property_summary_hybrid_trunc.set_downstream(extract_property_summary)
extract_property_summary.set_downstream(write_property_summary)
write_property_summary.set_downstream(delete_temp_property_summary)
delete_temp_property_summary.set_downstream(cleanup)

write_property_summary_new_prod.set_downstream(update_opa_properties_public)
update_opa_properties_public.set_downstream(cleanup)
# extract_opa_properties_public.set_downstream(trigger_carto_opa_properties_public_update)
# trigger_carto_opa_properties_public_update.set_downstream(delete_temp_opa_properties_public)
# delete_temp_opa_properties_public.set_downstream(cleanup)

write_property_summary_new_prod.set_downstream(update_opa_properties_public_pde)
update_opa_properties_public_pde.set_downstream(cleanup)
# extract_opa_properties_public_pde.set_downstream(trigger_carto_opa_properties_public_pde_update)
# trigger_carto_opa_properties_public_pde_update.set_downstream(delete_temp_opa_properties_public_pde)
# delete_temp_opa_properties_public_pde.set_downstream(cleanup)

# property_summary test:
update_property_summary_cama.set_upstream(write_land)
update_property_summary_cama.set_upstream(write_comdat)
update_property_summary_cama.set_upstream(write_comintext)
update_property_summary_cama.set_upstream(write_oby)
update_property_summary_cama.set_upstream(write_owndat)
update_property_summary_cama.set_upstream(write_sales)
update_property_summary_cama.set_upstream(write_dweldat)
update_property_summary_cama.set_upstream(write_legdat)
#update_property_summary_cama.set_upstream(write_homestead)

update_property_summary_cama.set_downstream(update_property_summary_hybrid_trunc)
update_property_summary_cama.set_downstream(extract_property_summary_new)
extract_property_summary_new.set_downstream(write_property_summary_new_prod)
extract_property_summary_new.set_downstream(write_property_summary_new_prod_db2)
write_property_summary_new_prod.set_downstream(delete_temp_property_summary_new)
write_property_summary_new_prod_db2.set_downstream(delete_temp_property_summary_new)
delete_temp_property_summary_new.set_downstream(cleanup)

# write_property_summary_hybrid.set_downstream(update_test_opa_properties_public)
# update_test_opa_properties_public.set_downstream(extract_test_opa_properties_public)
# extract_test_opa_properties_public.set_downstream(trigger_carto_test_opa_properties_public_update)
# trigger_carto_test_opa_properties_public_update.set_downstream(delete_temp_test_opa_properties_public)
# delete_temp_test_opa_properties_public.set_downstream(cleanup)

# write_property_summary_hybrid.set_downstream(update_test_opa_properties_public_pde)
# update_test_opa_properties_public_pde.set_downstream(extract_test_opa_properties_public_pde)
# extract_test_opa_properties_public_pde.set_downstream(trigger_carto_test_opa_properties_public_pde_update)
# trigger_carto_test_opa_properties_public_pde_update.set_downstream(delete_temp_test_opa_properties_public_pde)
# delete_temp_test_opa_properties_public_pde.set_downstream(cleanup)

# pin owner mailing:
update_pin_owner_mailing.set_upstream(write_owndat)
update_pin_owner_mailing.set_upstream(write_pardat)
update_pin_owner_mailing.set_upstream(write_sales)
update_pin_owner_mailing.set_downstream(extract_pin_owner_mailing)
extract_pin_owner_mailing.set_downstream(write_pin_owner_mailing)
write_pin_owner_mailing.set_downstream(cleanup)

# pin master
update_pardat_latest_prop_data.set_downstream(update_pin_master)
update_active_props.set_downstream(update_pin_master)
update_pin_master.set_downstream(extract_pin_master)
extract_pin_master.set_downstream(write_databridge_pin_master)
write_databridge_pin_master.set_downstream(delete_temp_pin_master)
delete_temp_pin_master.set_downstream(cleanup)


# pin source address std
update_pin_master.set_downstream(update_pin_source_address_from_view)
update_pin_source_address_from_view.set_downstream(extract_distinct_pin_source_addresses)
extract_distinct_pin_source_addresses.set_downstream(standardize_address_comps)
standardize_address_comps.set_downstream(write_std_pin_source_address_comps)
write_std_pin_source_address_comps.set_downstream(update_pin_source_address_plus_std_from_view)
update_pin_source_address_plus_std_from_view.set_downstream(extract_pin_source_address_plus_std)
extract_pin_source_address_plus_std.set_downstream(write_pin_source_address_plus_std)
write_pin_source_address_plus_std.set_downstream(delete_temp_pin_source_address_plus_std)
delete_temp_pin_source_address_plus_std.set_downstream(cleanup)
update_pin_source_address_plus_std_from_view.set_downstream(update_property_summary_cama)

# update pin cama owner for ais:
update_pin_owner_mailing.set_downstream(extract_opa_owners_cama_for_ais)
extract_opa_owners_cama_for_ais.set_downstream(write_opa_owners_cama_for_ais)
write_opa_owners_cama_for_ais.set_downstream(delete_temp_opa_owners_cama_for_ais)
write_opa_owners_cama_for_ais.set_downstream(cleanup)

# property codes
update_property_summary_cama.set_downstream(extract_property_codes_for_water)
extract_property_codes_for_water.set_downstream(write_property_codes_for_water)
write_property_codes_for_water.set_downstream(delete_temp_property_codes_for_water)
write_property_codes_for_water.set_downstream(cleanup)

# Deeds exchange:
update_pin_master.set_downstream(upsert_records_into_er_stage_transaction)
upsert_records_into_er_stage_transaction.set_downstream(update_er_stage_transaction_q9_review)
update_er_stage_transaction_q9_review.set_downstream(update_stage_transaction_status_deed_parsing)

update_pin_master.set_downstream(upsert_records_into_er_stage_parcel)
update_pin_master.set_downstream(upsert_records_into_er_stage_parties)
update_er_stage_transaction_status_for_pinned_records.set_upstream(update_pin_master)
update_er_stage_transaction_status_for_pinned_records.set_downstream(upsert_records_into_er_stage_transaction)
update_stage_transaction_status_deed_parsing.set_downstream(update_er_stage_parcel_q9_resolution_overridden)
update_er_stage_parcel_q9_resolution_overridden.set_downstream(insert_overridden_records_into_er_stage_parcel_overridden)
insert_overridden_records_into_er_stage_parcel_overridden.set_downstream(delete_overridden_records_from_er_stage_parcel)
delete_overridden_records_from_er_stage_parcel.set_downstream(upsert_records_into_er_stage_parcel)
upsert_records_into_er_stage_transaction.set_downstream(update_stage_transaction_status_deed_parsing)
upsert_records_into_er_stage_parcel.set_downstream(extract_er_stage_props_for_geocoding)
upsert_records_into_er_stage_parties.set_downstream(update_er_stage_parcel_pin_matching)
update_stage_transaction_status_deed_parsing.set_downstream(update_er_stage_parcel_pin_matching)
update_er_stage_parcel_pin_matching.set_downstream(update_er_stage_parcel_pin_pin_type)
update_er_stage_parcel_pin_pin_type.set_downstream(update_cama_property_deeds_new)

extract_er_stage_props_for_geocoding.set_downstream(geocode_er_stage_props)
geocode_er_stage_props.set_downstream(upsert_geocoded_er_stage_props)
upsert_geocoded_er_stage_props.set_downstream(update_er_stage_parcel_pin_matching)
update_er_stage_parcel_pin_matching.set_downstream(update_cama_property_deeds_new)
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

# Queue 9
make_staging >> backwards_oracle_sync >> update_opa_property_deeds_new_queue_9_pub >> refresh_ago >> cleanup
write_property_deeds_new_queue_9_updates >> update_opa_property_deeds_new_queue_9_pub
write_property_deeds_new_non_queue_9_updates >> write_property_deeds_new_queue_9_updates
make_staging >> backwards_q9_review_findings_sync >> extract_q9_review_findings >> write_q9_review_findings
update_cama_property_deeds_new.set_upstream(write_q9_review_findings)

# processed deeds
extract_processed_deeds << write_owndat
extract_processed_deeds << write_sales
extract_processed_deeds << upsert_records_into_er_stage_parcel
extract_processed_deeds << update_splcom_assessments
extract_processed_deeds >> write_processed_deeds
write_processed_deeds >> update_opa_property_deeds_new_queue_9_pub

# condo pins
extract_dor_condo_parcel_pin.set_upstream(update_pin_source_address_plus_std_from_view)
extract_dor_condo_parcel_pin.set_downstream(write_dor_condo_parcel_pin_to_tripoli)
write_dor_condo_parcel_pin_to_tripoli.set_downstream(cleanup)