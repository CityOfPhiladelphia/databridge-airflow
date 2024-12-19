import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.pin_source_address_std import check_address_comps
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
    'start_date': datetime(2019, 1, 15, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_cama_test_geopetl_v0', schedule_interval=None, default_args=default_args)

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
    if update_date_file and stmt_where:
        last_update_date = etl.fromcsv(update_date_file)[1][0]
        stmt_where=stmt_where.format(last_update_date=last_update_date)
        stmt = stmt + ' ' + stmt_where if last_update_date else stmt
    print(stmt)
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    #pg_hook.bulk_dump(table_name, csv_path)
    pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)


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

# TODO: change generalize stmt variable names:

update_table_from_view_stmt = '''BEGIN;
truncate table {table_name};
insert into {table_name} (select * from {view_name});
COMMIT;
'''

# get last updated timestamp from db oracle revenue interface:
get_last_asmt_update_from_oracle_for_revenue_stmt = '''select max(etl_modified_timestamp) as last_update from {table_name}'''

# get asmt updates since last updated timestamp from db2:
get_updates_from_db2_for_revenue_stmt = '''
    select {fields} 
    from (
        select distinct on (etl_hash) *
        from {table_name}
        ORDER BY etl_hash, etl_action_timestamp DESC NULLS LAST
    ) foo where etl_action <> 'delete'::text
'''
get_updates_from_db2_for_revenue_stmt_where = ''' and etl_action_timestamp > '{last_update_date}' '''


asmt_update_fields = '''tax_year,pin,opa_account_num,street_address,street_code,address_low,address_low_suffix,address_high,unit_num,asmt_mkt_val,asmt_tax_val,asmt_exmpt_val,exempt_code,zip,building_code,mailing_care_of,mailing_street_address,
mailing_address_1,mailing_address_2,mailing_city_state,mailing_zip,category_code,cert_action_date,taxable_land,taxable_building,exempt_land,exempt_building,cert_reason_code,transaction_num,etl_action_timestamp as etl_modified_timestamp'''

db2_asmt_updates_table_name = 'opa.assessment_cert_updates_history'

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

opa_account_num_history_update_fields = '''phl_acct_hist_det.parid AS pin,
    phl_acct_hist_det.taxyr AS tax_year,
    phl_acct_hist_det.new_alt_id AS new_opa_account_num,
    phl_acct_hist_det.old_alt_id AS old_opa_account_num,
    phl_acct_hist_det.trans_id AS transaction_num,
    phl_acct_hist_det.wen AS record_date,
    etl_action_timestamp as etl_modified_timestamp
'''

db2_opa_account_num_history_updates_table_name = 'cama.phl_acct_hist_det_history'

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from OPA

extract_splcom = GeopetlReadOperator(
    task_id='read_splcom',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/splcom.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.splcom',
    db_table_where='',
)

extract_pardat = GeopetlReadOperator(
    task_id='read_pardat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.pardat',
    db_table_where='',
)

extract_owndat = GeopetlReadOperator(
    task_id='read_owndat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owndat.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.owndat',
    db_table_where='',
)

extract_maildat = GeopetlReadOperator(
    task_id='read_maildat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/maildat.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.maildat',
    db_table_where='',
)

extract_sales =  GeopetlReadOperator(
    task_id='read_sales',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/sales.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.sales',
    db_table_where='',
)

extract_phl_acct_hist_det =  GeopetlReadOperator(
    task_id='read_phl_acct_hist_det',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_acct_hist_det.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.phl_acct_hist_det',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_splcom = GeopetlWriteOperator(
    task_id='write_splcom',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/splcom.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.splcom',
    db_table_where='',
)

write_pardat = GeopetlWriteOperator(
    task_id='write_pardat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/pardat.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.pardat',
    db_table_where='',
)

write_owndat = GeopetlWriteOperator(
    task_id='write_owndat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owndat.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.owndat',
    db_table_where='',
)

write_maildat = GeopetlWriteOperator(
    task_id='write_maildat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/maildat.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.maildat',
    db_table_where='',
)

write_sales = GeopetlWriteOperator(
    task_id='write_sales',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/sales.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.sales',
    db_table_where='',
)

write_phl_acct_hist_det = GeopetlWriteOperator(
    task_id='write_phl_acct_hist_det',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_acct_hist_det.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.phl_acct_hist_det',
    db_table_where='',
)

# -----------------------------------------------------------------
# Cleanup temp files

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

delete_temp_sales = PythonOperator(
    task_id='delete_temp_sales',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'sales.csv',},
)

delete_temp_phl_acct_hist_det = PythonOperator(
    task_id='delete_temp_phl_acct_hist_det',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'phl_acct_hist_det.csv',},
)


# -----------------------------------------------------------------
# Update hashes
#

update_splcom_hash = PythonOperator(
    task_id='update_splcom_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'splcom', 'hash_field': 'etl_hash'},
)

update_pardat_hash = PythonOperator(
    task_id='update_pardat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'pardat', 'hash_field': 'etl_hash'},
)

update_owndat_hash = PythonOperator(
    task_id='update_owndat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'owndat', 'hash_field': 'etl_hash'},
)

update_maildat_hash = PythonOperator(
    task_id='update_maildat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'maildat', 'hash_field': 'etl_hash'},
)

update_sales_hash = PythonOperator(
    task_id='update_sales_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_phl_acct_hist_det_hash = PythonOperator(
    task_id='update_phl_acct_hist_det_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_acct_hist_det', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories
#

update_splcom_history = PythonOperator(
    task_id='update_splcom_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'splcom', 'hash_field': 'etl_hash'},
)

update_pardat_history = PythonOperator(
    task_id='update_pardat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'pardat', 'hash_field': 'etl_hash'},
)

update_owndat_history = PythonOperator(
    task_id='update_owndat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'owndat', 'hash_field': 'etl_hash'},
)

update_maildat_history = PythonOperator(
    task_id='update_maildat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'maildat', 'hash_field': 'etl_hash'},
)

update_sales_history = PythonOperator(
    task_id='update_sales_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_phl_acct_hist_det_history = PythonOperator(
    task_id='update_phl_acct_hist_det_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_acct_hist_det', 'hash_field': 'etl_hash'},
)

#--------------------------------------------------------------------------------------------------------------------------
# Additional tasks:

# update cama.pardat_latest_prop_data from view:
pardat_latest_prop_data_table_name = 'cama_test.pardat_latest_prop_data'
pardat_latest_prop_data_view_name = 'cama_test.vw_pardat_latest_prop_data'
update_pardat_latest_prop_data = PythonOperator(
    task_id='update_pardat_latest_prop_data',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=pardat_latest_prop_data_table_name, view_name=pardat_latest_prop_data_view_name)},
)

# update property.party from view:
property_party_table_name = 'property_test.party'
property_party_view_name = 'property_test.vw_party_v3'
update_property_party = PythonOperator(
    task_id='update_property_party',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_party_table_name,view_name=property_party_view_name)},
)

pin_source_address_table_name = 'property_test.pin_source_address'
pin_source_address_view_name = 'property_test.vw_pin_source_address'

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
db2_source_address_std_table_name = 'property_test.pin_source_address_std'
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

pin_source_address_plus_std_table_name = 'property_test.pin_source_address_plus_std'
pin_source_address_plus_std_view_name = 'property_test.vw_pin_source_address_plus_std'
pin_source_address_plus_std_temp_table_name = 'temp_test_pin_source_address_plus_std'
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


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

#make_staging >> extract_splcom >> write_to_databridge >> cleanup
#
extract_splcom.set_upstream(make_staging)
extract_pardat.set_upstream(make_staging)
extract_owndat.set_upstream(make_staging)
extract_maildat.set_upstream(make_staging)
extract_sales.set_upstream(make_staging)
extract_phl_acct_hist_det.set_upstream(make_staging)
extract_splcom.set_downstream(write_splcom)
extract_pardat.set_downstream(write_pardat)
extract_owndat.set_downstream(write_owndat)
extract_maildat.set_downstream(write_maildat)
extract_sales.set_downstream(write_sales)
extract_phl_acct_hist_det.set_downstream(write_phl_acct_hist_det)
write_splcom.set_downstream(update_splcom_hash)
write_splcom.set_downstream(delete_temp_splcom)
write_pardat.set_downstream(update_pardat_hash)
write_pardat.set_downstream(delete_temp_pardat)
write_pardat.set_downstream(update_pardat_latest_prop_data)
update_pardat_latest_prop_data.set_downstream(update_pin_source_address_from_view)
update_pin_source_address_from_view.set_downstream(extract_distinct_pin_source_addresses)
extract_distinct_pin_source_addresses.set_downstream(standardize_address_comps)
standardize_address_comps.set_downstream(write_std_pin_source_address_comps)
write_std_pin_source_address_comps.set_downstream(update_pin_source_address_plus_std_from_view)
update_pin_source_address_plus_std_from_view.set_downstream(cleanup)
write_owndat.set_downstream(update_owndat_hash)
write_owndat.set_downstream(delete_temp_owndat)
write_maildat.set_downstream(update_maildat_hash)
write_maildat.set_downstream(delete_temp_maildat)
write_sales.set_downstream(update_sales_hash)
write_sales.set_downstream(delete_temp_sales)
write_phl_acct_hist_det.set_downstream(update_phl_acct_hist_det_hash)
write_phl_acct_hist_det.set_downstream(delete_temp_phl_acct_hist_det)
update_splcom_hash.set_downstream(update_splcom_history)
update_pardat_hash.set_downstream(update_pardat_history)
update_owndat_hash.set_downstream(update_owndat_history)
update_maildat_hash.set_downstream(update_maildat_history)
update_sales_hash.set_downstream(update_sales_history)
update_phl_acct_hist_det_hash.set_downstream(update_phl_acct_hist_det_history)
update_splcom_history.set_downstream(cleanup)
update_pardat_history.set_downstream(cleanup)
update_owndat_history.set_downstream(cleanup)
update_maildat_history.set_downstream(cleanup)
update_sales_history.set_downstream(cleanup)
update_phl_acct_hist_det_history.set_downstream(cleanup)
update_property_party.set_upstream(write_pardat)
update_property_party.set_upstream(write_owndat)
update_property_party.set_downstream(cleanup)
