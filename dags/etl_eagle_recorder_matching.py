import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.hooks import GeopetlHook
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
from datetime import datetime, timedelta #, timezone
from pytz import timezone 
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

pipeline = DAG('etl_eagle_recorder_address_matching_v0', schedule_interval='0 5 * * *', default_args=default_args)

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

insert_audit_file_upload_history_stmt = '''BEGIN;
insert into audit.file_upload_history (schema_name, table_name, upload_file_name, upload_file_date, last_etl_timestamp_processed, upload_interface)
values('{last_update_schema_name}', '{last_update_table_name}', '{upload_file_name}', '{upload_file_date}', '{last_update_date}', '{upload_interface}');
COMMIT;
'''

# get last updated timestamp from audit table:
get_last_update_from_audit_stmt = ''' select max(last_etl_timestamp_processed) as last_update from audit.file_upload_history where schema_name = '{last_update_schema_name}' and table_name = '{last_update_table_name}' '''

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract and perform standardization report for distinct street_addresses:

er_property_addresses_view_name = 'dor.vw_er_property_address'

read_distinct_er_property_addresses_stmt = '''
select distinct concatenated_street_address as street_address from {er_property_addresses_view_name}
'''.format(er_property_addresses_view_name=er_property_addresses_view_name)


extract_distinct_er_property_addresses = PythonOperator(
    task_id='read_distinct_er_property_addresses',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': er_property_addresses_view_name, 'stmt': read_distinct_er_property_addresses_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}//distinct_er_property_addresses.csv'}
)

standardize_address_comps = PythonOperator(
    task_id='standardize_address_comps',
    dag=pipeline,
    python_callable=check_address_comps,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}','infile_name': 'distinct_er_property_addresses.csv', 'outfile_suffix':'_std'},
)

# Write to databridge2:
db2_source_address_std_table_name = 'dor.er_property_addresses_std'
write_er_property_addresses_std_stmt = '''
BEGIN;
truncate table {table_name};
COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true);
COMMIT;
'''

write_std_er_property_address_comps = PythonOperator(
    task_id='write_std_er_property_address_comps',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_source_address_std_table_name, 'stmt': write_er_property_addresses_std_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/distinct_er_property_addresses_std.csv'},
)

# update er_property_matching table from view using upsert:
er_property_matching_table_schema_name = 'dor.er_property_matching'
er_property_matching_view_schema_name = 'dor.vw_er_property_matching'
update_er_property_matching_stmt = update_er_property_matching_sql.format(er_property_matching_table_schema_name = er_property_matching_table_schema_name, er_property_matching_view_schema_name = er_property_matching_view_schema_name)
update_er_property_matching = PythonOperator(
    task_id='update_er_property_matching',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_property_matching_stmt},
)

# update er_rtt_complete table from view:
er_rtt_complete_table_schema_name = 'dor.er_rtt_complete'
er_rtt_complete_view_schema_name = 'dor.vw_er_rtt_complete'
update_er_rtt_complete_stmt = update_er_rtt_complete_sql.format(er_rtt_complete_table_schema_name = er_rtt_complete_table_schema_name,  er_rtt_complete_view_schema_name  = er_rtt_complete_view_schema_name)
update_er_rtt_complete = PythonOperator(
    task_id='update_er_rtt_complete',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_er_rtt_complete_stmt},
)

# update rtt_summary table from view using upsert:
rtt_summary_table_schema_name = 'dor.rtt_summary'
rtt_summary_view_schema_name = 'dor.vw_rtt_summary'
update_dor_rtt_summary_stmt = update_dor_rtt_summary_sql
update_rtt_summary = PythonOperator(
    task_id='update_rtt_summary',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_dor_rtt_summary_stmt},
)

#extract_rtt_summary = GeopetlReadOperator(
#    task_id='read_rtt_summary',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/rtt_summary.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.rtt_summary',
#    db_table_where='',
#)

#write_rtt_summary = GeopetlWriteOperator(
#    task_id='write_rtt_summary',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/rtt_summary.csv',
#    db_conn_id='databridge-dor',
#    db_table_name='rtt_summary',
#    db_table_where='',
#)

#delete_temp_rtt_summary = PythonOperator(
#    task_id='delete_temp_rtt_summary',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'rtt_summary.csv',},
#)

# update deeds_for_revenue table from view using upsert:
deeds_for_revenue_table_schema_name = 'dor.deeds_for_revenue'
deeds_for_revenue_view_schema_name = 'dor.vw_deeds_for_revenue'
update_deeds_for_revenue_stmt = update_deeds_for_revenue_sql.format(deeds_for_revenue_table_schema_name = deeds_for_revenue_table_schema_name, deeds_for_revenue_view_schema_name = deeds_for_revenue_view_schema_name)
update_deeds_for_revenue = PythonOperator(
    task_id='update_deeds_for_revenue',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_deeds_for_revenue_stmt},
)

# get last update ts for audit for deeds_for_revenue table:
last_deeds_for_revenue_update_ts_stmt = ''' select max(etl_modified_timestamp) as last_update from audit.change_history_for_revenue where schemaname = 'dor' and tabname = 'deeds_for_revenue' and  lower(operation) = 'insert' '''
extract_last_deeds_for_revenue_update_ts_for_audit = GeopetlReadOperator(
    task_id='read_last_deeds_for_revenue_update_ts_for_audit',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_deeds_for_revenue_update_ts_for_audit.csv',
    db_conn_id='databridge2',
    db_table_name='',
    db_table_where='',
    db_sql=last_deeds_for_revenue_update_ts_stmt
)

# extract deeds for revenue updates for sftp:
deeds_for_revenue_schema_table_name = 'dor.deeds_for_revenue'
get_deeds_for_revenue_update_for_sftp_stmt = get_deeds_for_revenue_updates_for_sftp_sql
extract_deeds_for_revenue_updates_for_sftp = PythonOperator(
    task_id='read_deeds_for_revenue_updates_for_sftp',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': deeds_for_revenue_schema_table_name, 'stmt': get_deeds_for_revenue_update_for_sftp_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/deeds_for_revenue_updates_for_sftp.csv'},
)

# Upload to deeds for revenue updates to sftp:
# localize timestamp
eastern = timezone('US/Eastern')
deeds_for_revenue_sftp_upload_timestamptz = datetime.now(eastern)
#deeds_for_revenue_sftp_upload_timestamptz = datetime.now(timezone.utc)
#deeds_for_revenue_sftp_upload_date = str(datetime.date(deeds_for_revenue_sftp_upload_timestamptz))
deeds_for_revenue_sftp_file_name = 'deeds_for_revenue_' + str(deeds_for_revenue_sftp_upload_timestamptz).replace(' ', '_').replace(':','') + '.csv'
deeds_for_revenue_sftp_file_path = 'Records_Documents/' + deeds_for_revenue_sftp_file_name
upload_deeds_for_revenue_updates_to_sftp = SFTPOperator(
    task_id='upload_deeds_for_revenue_updates_to_sftp',
    ssh_conn_id='sftp-databridge-revenue-prod',
    dag=pipeline,
    local_filepath='{{ ti.xcom_pull("make_staging") }}/deeds_for_revenue_updates_for_sftp.csv',
    remote_filepath=deeds_for_revenue_sftp_file_path,
    operation="put",
    create_intermediate_dirs=False,
)

# Insert audit record for deeds_for_revenue sftp update:
deeds_for_revenue_updates_schema_name=deeds_for_revenue_schema_table_name.split('.')[0]
deeds_for_revenue_updates_table_name=deeds_for_revenue_schema_table_name.split('.')[1]
insert_deeds_for_revenue_sftp_update_audit_record =  PythonOperator(
    task_id='insert_deeds_for_revenue_sftp_update_audit_record',
    dag=pipeline,
    python_callable=insert_audit_record,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'last_update_schema_name': deeds_for_revenue_updates_schema_name, 'last_update_table_name': deeds_for_revenue_updates_table_name,  'upload_file_name': deeds_for_revenue_sftp_file_name, 'upload_file_date': deeds_for_revenue_sftp_upload_timestamptz, 'upload_interface': 'sftp', 'stmt': insert_audit_file_upload_history_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/last_deeds_for_revenue_update_ts_for_audit.csv'},
)

#### Deeds for Revenue for BASIS2:

# get last update ts for audit for deeds_for_revenue table for basis2:
last_deeds_for_revenue_for_basis2_update_ts_stmt = ''' select max(etl_modified_timestamp) as last_update from audit.change_history_for_revenue where schemaname = 'dor' and tabname = 'deeds_for_revenue' and  lower(operation) = 'insert' '''
extract_last_deeds_for_revenue_for_basis2_update_ts_for_audit = GeopetlReadOperator(
    task_id='read_last_deeds_for_revenue_for_basis2_update_ts_for_audit',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_deeds_for_revenue_for_basis2_update_ts_for_audit.csv',
    db_conn_id='databridge2',
    db_table_name='',
    db_table_where='',
    db_sql=last_deeds_for_revenue_for_basis2_update_ts_stmt
)

# extract deeds for revenue for basis2 updates for sftp:
deeds_for_revenue_for_basis2_schema_table_name = 'dor.deeds_for_revenue'
get_deeds_for_revenue_for_basis2_update_for_sftp_stmt = get_deeds_for_revenue_for_basis2_updates_for_sftp_sql
extract_deeds_for_revenue_for_basis2_updates_for_sftp = PythonOperator(
    task_id='read_deeds_for_revenue_for_basis2_updates_for_sftp',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': deeds_for_revenue_schema_table_name, 'stmt': get_deeds_for_revenue_for_basis2_update_for_sftp_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/deeds_for_revenue_for_basis2_updates_for_sftp.csv'},
)

# Upload to deeds for revenue for Basis2 updates to sftp:
# localize timestamp
eastern = timezone('US/Eastern')
deeds_for_revenue_for_basis2_sftp_upload_timestamptz = datetime.now(eastern)
#deeds_for_revenue_sftp_upload_timestamptz = datetime.now(timezone.utc)
#deeds_for_revenue_sftp_upload_date = str(datetime.date(deeds_for_revenue_sftp_upload_timestamptz))
deeds_for_revenue_for_basis2_sftp_file_name = 'deeds_for_basis2_' + str(deeds_for_revenue_for_basis2_sftp_upload_timestamptz).replace(' ', '_').replace(':','') + '.csv'
# deeds_for_revenue_for_basis2_sftp_file_path = 'Test-Ocean_Records/IN/' + deeds_for_revenue_for_basis2_sftp_file_name
deeds_for_revenue_for_basis2_sftp_file_path = 'Prod_Records/IN/' + deeds_for_revenue_for_basis2_sftp_file_name

upload_deeds_for_revenue_for_basis2_updates_to_sftp = SFTPOperator(
    task_id='upload_deeds_for_revenue_for_basis2_updates_to_sftp',
    ssh_conn_id='sftp-databridge-basis2-prod',
    dag=pipeline,
    local_filepath='{{ ti.xcom_pull("make_staging") }}/deeds_for_revenue_for_basis2_updates_for_sftp.csv',
    remote_filepath=deeds_for_revenue_for_basis2_sftp_file_path,
    operation="put",
    create_intermediate_dirs=False,
)

# Insert audit record for deeds_for_revenue_for_basis2 sftp update:
deeds_for_revenue_for_basis2_updates_schema_name=deeds_for_revenue_schema_table_name.split('.')[0]
deeds_for_revenue_for_basis2_updates_table_name=deeds_for_revenue_schema_table_name.split('.')[1] + '_for_basis2'
insert_deeds_for_revenue_for_basis2_sftp_update_audit_record =  PythonOperator(
    task_id='insert_deeds_for_revenue_for_basis2_sftp_update_audit_record',
    dag=pipeline,
    python_callable=insert_audit_record,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'last_update_schema_name': deeds_for_revenue_for_basis2_updates_schema_name, 'last_update_table_name': deeds_for_revenue_for_basis2_updates_table_name,  'upload_file_name': deeds_for_revenue_for_basis2_sftp_file_name, 'upload_file_date': deeds_for_revenue_for_basis2_sftp_upload_timestamptz, 'upload_interface': 'sftp', 'stmt': insert_audit_file_upload_history_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/last_deeds_for_revenue_for_basis2_update_ts_for_audit.csv'},
)

# Notary Fraud Deeds for Revenue
# update notary_fraud_affidavit_deeds_for_revenue table from view using upsert:
notary_fraud_affidavit_deeds_for_revenue_table_schema_name = 'dor.notary_fraud_affidavit_deeds_for_revenue'
notary_fraud_affidavit_deeds_for_revenue_view_schema_name = 'dor.vw_notary_fraud_affidavit_deeds_for_revenue'
update_notary_fraud_affidavit_deeds_for_revenue_stmt = update_notary_fraud_affidavit_deeds_for_revenue_sql.format(notary_fraud_affidavit_deeds_for_revenue_table_schema_name = notary_fraud_affidavit_deeds_for_revenue_table_schema_name, notary_fraud_affidavit_deeds_for_revenue_view_schema_name = notary_fraud_affidavit_deeds_for_revenue_view_schema_name)
update_notary_fraud_affidavit_deeds_for_revenue = PythonOperator(
    task_id='update_notary_fraud_affidavit_deeds_for_revenue',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_notary_fraud_affidavit_deeds_for_revenue_stmt},
)

# get last update ts for audit for notary_fraud_affidavit_deeds_for_revenue table:
last_notary_fraud_affidavit_deeds_for_revenue_update_ts_stmt = ''' select max(etl_modified_timestamp) as last_update from audit.change_history_for_revenue where schemaname = 'dor' and tabname = 'notary_fraud_affidavit_deeds_for_revenue' and  lower(operation) = 'insert' '''
extract_last_notary_fraud_affidavit_deeds_for_revenue_update_ts_for_audit = GeopetlReadOperator(
    task_id='read_last_notary_fraud_affidavit_deeds_for_revenue_update_ts_for_audit',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_notary_fraud_affidavit_deeds_for_revenue_update_ts_for_audit.csv',
    db_conn_id='databridge2',
    db_table_name='',
    db_table_where='',
    db_sql=last_notary_fraud_affidavit_deeds_for_revenue_update_ts_stmt
)

# extract notary_fraud_affidavit_deeds_for_revenue updates for sftp:
notary_fraud_affidavit_deeds_for_revenue_schema_table_name = 'dor.notary_fraud_affidavit_deeds_for_revenue'
get_notary_fraud_affidavit_deeds_for_revenue_update_for_sftp_stmt = get_notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp_sql
extract_notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp = PythonOperator(
    task_id='read_notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': notary_fraud_affidavit_deeds_for_revenue_schema_table_name, 'stmt': get_notary_fraud_affidavit_deeds_for_revenue_update_for_sftp_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp.csv'},
)

# Upload to notary_fraud_affidavit_deeds_for_revenue updates to sftp:
# localize timestamp
eastern = timezone('US/Eastern')
notary_fraud_affidavit_deeds_for_revenue_sftp_upload_timestamptz = datetime.now(eastern)
notary_fraud_affidavit_deeds_for_revenue_sftp_file_name = 'notary_fraud_affidavit_deeds_for_revenue_' + str(notary_fraud_affidavit_deeds_for_revenue_sftp_upload_timestamptz).replace(' ', '_').replace(':','') + '.csv'
notary_fraud_affidavit_deeds_for_revenue_sftp_file_path = 'Records_Fraud/' + notary_fraud_affidavit_deeds_for_revenue_sftp_file_name
upload_notary_fraud_affidavit_deeds_for_revenue_updates_to_sftp = SFTPOperator(
    task_id='upload_notary_fraud_affidavit_deeds_for_revenue_updates_to_sftp',
    ssh_conn_id='sftp-databridge-revenue-prod',
    dag=pipeline,
    local_filepath='{{ ti.xcom_pull("make_staging") }}/notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp.csv',
    remote_filepath=notary_fraud_affidavit_deeds_for_revenue_sftp_file_path,
    operation="put",
    create_intermediate_dirs=False,
)

# Insert audit record for deeds_for_revenue sftp update:
notary_fraud_affidavit_deeds_for_revenue_updates_schema_name=notary_fraud_affidavit_deeds_for_revenue_schema_table_name.split('.')[0]
notary_fraud_affidavit_deeds_for_revenue_updates_table_name=notary_fraud_affidavit_deeds_for_revenue_schema_table_name.split('.')[1]
insert_notary_fraud_affidavit_deeds_for_revenue_sftp_update_audit_record =  PythonOperator(
    task_id='insert_notary_fraud_affidavit_deeds_for_revenue_sftp_update_audit_record',
    dag=pipeline,
    python_callable=insert_audit_record,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'last_update_schema_name': notary_fraud_affidavit_deeds_for_revenue_updates_schema_name, 'last_update_table_name': notary_fraud_affidavit_deeds_for_revenue_updates_table_name,  'upload_file_name': notary_fraud_affidavit_deeds_for_revenue_sftp_file_name, 'upload_file_date': deeds_for_revenue_sftp_upload_timestamptz, 'upload_interface': 'sftp', 'stmt': insert_audit_file_upload_history_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/last_notary_fraud_affidavit_deeds_for_revenue_update_ts_for_audit.csv'},
)


## TODO - refactor from here:#
#
#pin_source_address_plus_std_table_name = 'property.pin_source_address_plus_std'
#pin_source_address_plus_std_view_name = 'property.vw_pin_source_address_plus_std'
#pin_source_address_plus_std_temp_table_name = 'temp_pin_source_address_plus_std'
#upsert_pin_source_address_plus_std_stmt = upsert_pin_source_address_plus_std_sql.format(pin_source_address_plus_std_table_schema_name=pin_source_address_plus_std_table_name, pin_source_address_plus_std_view_schema_name=pin_source_address_plus_std_view_name)
#update_pin_source_address_plus_std_stmt = '''
#BEGIN;
#create temp table {pin_source_address_plus_std_temp_table_name} as select * from {pin_source_address_plus_std_view_name};
#{upsert_stmt};
#delete from {pin_source_address_plus_std_table_name} main where (pin, address_source) in (
#        select pin, address_source from {pin_source_address_plus_std_temp_table_name}
#        except
#        select pin, address_source from {pin_source_address_plus_std_temp_table_name}
#);
#COMMIT;
#'''.format(pin_source_address_plus_std_table_name=pin_source_address_plus_std_table_name, pin_source_address_plus_std_view_name=pin_source_address_plus_std_view_name, pin_source_address_plus_std_temp_table_name=pin_source_address_plus_std_temp_table_name, upsert_stmt=upsert_pin_source_address_plus_std_stmt)
#
## update pin_source_address_plus_std table from view using upsert:
#update_pin_source_address_plus_std_from_view = PythonOperator(
#    task_id='update_pin_source_address_plus_std_from_view',
#    dag=pipeline,
#    python_callable=update_postgres,
#    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_pin_source_address_plus_std_stmt},
#)
#
## extract updated table:
#extract_pin_source_address_plus_std = GeopetlReadOperator(
#    task_id='read_pin_source_address_plus_std',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/pin_source_address_plus_std.csv',
#    db_conn_id='databridge2',
#    db_table_name='property.pin_source_address_plus_std',
#    db_table_where='',
#)
#
#write_pin_source_address_plus_std = GeopetlWriteOperator(
#    task_id='write_pin_source_address_plus_std',
#    dag=pipeline,
#    csv_path = '{{ ti.xcom_pull("make_staging") }}/pin_source_address_plus_std.csv',
#    db_conn_id='databridge-gsg',
#    db_table_name='GIS_GSG.PIN_SOURCE_ADDRESS_STD',
#    db_table_where = '',
#    append=False,
#)
#
#delete_temp_distinct_er_property_addresses_std = PythonOperator(
#    task_id='delete_temp_er_property_addresses_std',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'distinct_er_property_addresses_std.csv',},
#)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

make_staging.set_downstream(extract_distinct_er_property_addresses)
extract_distinct_er_property_addresses.set_downstream(standardize_address_comps)
standardize_address_comps.set_downstream(write_std_er_property_address_comps)
write_std_er_property_address_comps.set_downstream(update_er_property_matching)
update_er_property_matching.set_downstream(update_er_rtt_complete)
update_er_rtt_complete.set_downstream(update_rtt_summary)
update_rtt_summary.set_downstream(cleanup)
#update_rtt_summary.set_downstream(extract_rtt_summary)
#extract_rtt_summary.set_downstream(write_rtt_summary)
#write_rtt_summary.set_downstream(delete_temp_rtt_summary)
#write_rtt_summary.set_downstream(cleanup)

update_er_rtt_complete.set_downstream(update_deeds_for_revenue)
update_deeds_for_revenue.set_downstream(extract_deeds_for_revenue_updates_for_sftp)
extract_deeds_for_revenue_updates_for_sftp.set_downstream(extract_last_deeds_for_revenue_update_ts_for_audit)
extract_deeds_for_revenue_updates_for_sftp.set_downstream(upload_deeds_for_revenue_updates_to_sftp)
upload_deeds_for_revenue_updates_to_sftp.set_downstream(insert_deeds_for_revenue_sftp_update_audit_record)
extract_last_deeds_for_revenue_update_ts_for_audit.set_downstream(insert_deeds_for_revenue_sftp_update_audit_record)
insert_deeds_for_revenue_sftp_update_audit_record.set_downstream(cleanup)

update_deeds_for_revenue.set_downstream(extract_deeds_for_revenue_for_basis2_updates_for_sftp)
extract_deeds_for_revenue_for_basis2_updates_for_sftp.set_downstream(extract_last_deeds_for_revenue_for_basis2_update_ts_for_audit)
extract_deeds_for_revenue_for_basis2_updates_for_sftp.set_downstream(upload_deeds_for_revenue_for_basis2_updates_to_sftp)
upload_deeds_for_revenue_for_basis2_updates_to_sftp.set_downstream(insert_deeds_for_revenue_for_basis2_sftp_update_audit_record)
extract_last_deeds_for_revenue_for_basis2_update_ts_for_audit.set_downstream(insert_deeds_for_revenue_for_basis2_sftp_update_audit_record)
insert_deeds_for_revenue_for_basis2_sftp_update_audit_record.set_downstream(cleanup)

update_deeds_for_revenue.set_downstream(update_notary_fraud_affidavit_deeds_for_revenue)
update_notary_fraud_affidavit_deeds_for_revenue.set_downstream(extract_notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp)
extract_notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp.set_downstream(extract_last_notary_fraud_affidavit_deeds_for_revenue_update_ts_for_audit)
extract_notary_fraud_affidavit_deeds_for_revenue_updates_for_sftp.set_downstream(upload_notary_fraud_affidavit_deeds_for_revenue_updates_to_sftp)
upload_notary_fraud_affidavit_deeds_for_revenue_updates_to_sftp.set_downstream(insert_notary_fraud_affidavit_deeds_for_revenue_sftp_update_audit_record)
extract_last_notary_fraud_affidavit_deeds_for_revenue_update_ts_for_audit.set_downstream(insert_notary_fraud_affidavit_deeds_for_revenue_sftp_update_audit_record)
insert_notary_fraud_affidavit_deeds_for_revenue_sftp_update_audit_record.set_downstream(cleanup)
