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

pipeline = DAG('etl_cama_test_for_revenue_v0', schedule_interval=None, default_args=default_args)

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
    db_conn_id='cama-test',
    db_table_name='philly_test.phl_adrcrtval_vw',
    db_table_where='',
)

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

extract_dweldat = GeopetlReadOperator(
    task_id='read_dweldat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dweldat.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.dweldat',
    db_table_where='',
)

extract_asmt_all = GeopetlReadOperator(
    task_id='read_asmt_all',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.asmt_all',
    db_sql='select parid, taxyr, wen, trans_id, tot03, tot08, tot09, tot10, tot11, reascd, seq, current_timestamp as etl_read_timestamp from philly_test.asmt_all',
)

extract_exdet = GeopetlReadOperator(
    task_id='read_exdet',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/exdet.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.exdet',
    db_sql='select parid, taxyr, excode, begdt, wen, current_timestamp as etl_read_timestamp from philly_test.exdet',
)

extract_legdat = GeopetlReadOperator(
    task_id='read_legdat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/legdat.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.legdat',
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

extract_phl_asmt =  GeopetlReadOperator(
    task_id='read_phl_asmt',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_asmt.csv',
    db_conn_id='cama-test',
    db_table_name='philly_test.phl_asmt',
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

extract_asmt_cert_updates =  GeopetlReadOperator(
    task_id='read_asmt_cert_updates',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt_cert_updates.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.vw_assessment_cert_updates_for_revenue',
    db_table_where='',
)

#
# ----------------------------------------------------
# Write extracted files to Databridge

write_phl_adrcrtval_vw = GeopetlWriteOperator(
    task_id='write_phl_adrcrtval_vw',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_adrcrtval_vw.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.phl_adrcrtval_vw',
    db_table_where='',
)

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

write_dweldat = GeopetlWriteOperator(
    task_id='write_dweldat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dweldat.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.dweldat',
    db_table_where='',
)

write_asmt = GeopetlWriteOperator(
    task_id='write_asmt',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.asmt',
    db_table_where='',
)

write_exdet = GeopetlWriteOperator(
    task_id='write_exdet',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/exdet.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.exdet',
    db_table_where='',
)

write_legdat = GeopetlWriteOperator(
    task_id='write_legdat',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/legdat.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.legdat',
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

write_phl_asmt = GeopetlWriteOperator(
    task_id='write_phl_asmt',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/phl_asmt.csv',
    db_conn_id='databridge2',
    db_table_name='cama_test.phl_asmt',
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

write_asmt_cert_updates = GeopetlWriteOperator(
    task_id='write_asmt_cert_updates',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/asmt_cert_updates.csv',
    db_conn_id='databridge2',
    db_table_name='opa_test.assessment_cert_updates',
    db_table_where='',
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

delete_temp_asmt_cert_updates = PythonOperator(
    task_id='delete_temp_asmt_cert_updates',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'asmt_cert_updates.csv',},
)

# -----------------------------------------------------------------
# Update hashes
#

update_phl_adrcrtval_vw_hash = PythonOperator(
    task_id='update_phl_adrcrtval_v2_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_adrcrtval_vw', 'hash_field': 'etl_hash'},
)


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

update_dweldat_hash = PythonOperator(
    task_id='update_dweldat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'dweldat', 'hash_field': 'etl_hash'},
)

update_asmt_hash = PythonOperator(
    task_id='update_asmt_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'asmt', 'hash_field': 'etl_hash'},
)

update_exdet_hash = PythonOperator(
    task_id='update_exdet_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'exdet', 'hash_field': 'etl_hash'},
)

update_legdat_hash = PythonOperator(
    task_id='update_legdat_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'legdat', 'hash_field': 'etl_hash'},
)

update_sales_hash = PythonOperator(
    task_id='update_sales_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_phl_asmt_hash = PythonOperator(
    task_id='update_phl_asmt_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_asmt', 'hash_field': 'etl_hash'},
)

update_phl_acct_hist_det_hash = PythonOperator(
    task_id='update_phl_acct_hist_det_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_acct_hist_det', 'hash_field': 'etl_hash'},
)

update_asmt_cert_updates_hash = PythonOperator(
    task_id='update_asmt_cert_updates_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa_test', 'table_name': 'assessment_cert_updates', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories
#

update_phl_adrcrtval_vw_history = PythonOperator(
    task_id='update_phl_adrcrtval_vw_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_adrcrtval_vw', 'hash_field': 'etl_hash'},
)


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

update_dweldat_history = PythonOperator(
    task_id='update_dweldat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'dweldat', 'hash_field': 'etl_hash'},
)

update_asmt_history = PythonOperator(
    task_id='update_asmt_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'asmt', 'hash_field': 'etl_hash'},
)

update_exdet_history = PythonOperator(
    task_id='update_exdet_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'exdet', 'hash_field': 'etl_hash'},
)

update_legdat_history = PythonOperator(
    task_id='update_legdat_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'legdat', 'hash_field': 'etl_hash'},
)

update_sales_history = PythonOperator(
    task_id='update_sales_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_phl_asmt_history = PythonOperator(
    task_id='update_phl_asmt_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_asmt', 'hash_field': 'etl_hash'},
)

update_phl_acct_hist_det_history = PythonOperator(
    task_id='update_phl_acct_hist_det_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'cama_test', 'table_name': 'phl_acct_hist_det', 'hash_field': 'etl_hash'},
)

update_asmt_cert_updates_history = PythonOperator(
    task_id='update_asmt_cert_updates_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa_test', 'table_name': 'assessment_cert_updates', 'hash_field': 'etl_hash'},
)

#--------------------------------------------------------------------------------------------------------------------------
# Additional tasks:

asmt_update_fields = '''tax_year,pin,opa_account_num,street_address,street_code,address_low,address_low_suffix,address_high,unit_num,asmt_mkt_val,asmt_tax_val,asmt_exmpt_val,exempt_code,zip,building_code,mailing_care_of,mailing_street_address,
mailing_address_1,mailing_address_2,mailing_city_state,mailing_zip,category_code,cert_action_date,taxable_land,taxable_building,exempt_land,exempt_building,cert_reason_code,transaction_num,etl_action_timestamp as etl_modified_timestamp, owner_1, owner_2'''


db2_asmt_updates_table_name = 'opa_test.assessment_cert_updates_history'

extract_last_asmt_update_from_oracle_for_revenue = GeopetlReadOperator(
    task_id='read_last_asmt_update_from_oracle_for_revenue',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_asmt_update_timestamp.csv',
    db_conn_id='gisdbp_t_gis_opa',
    db_table_name='assessment_cert_update',
    db_table_where='',
    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='assessment_cert_update')
)

extract_asmt_updates_for_revenue = PythonOperator(
    task_id='read_asmt_updates_for_revenue',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_asmt_updates_table_name, 'stmt': get_updates_from_db2_for_revenue_stmt.format(table_name=db2_asmt_updates_table_name, fields=asmt_update_fields),
        'stmt_where': get_updates_from_db2_for_revenue_stmt_where},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/asmt_updates_for_revenue.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_asmt_update_timestamp.csv'},
)

write_asmt_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
    task_id='write_asmt_updates_for_revenue',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/asmt_updates_for_revenue.csv',
    db_conn_id='gisdbp_t_gis_opa',
    db_table_name='GIS_OPA.ASSESSMENT_CERT_UPDATE',
    db_table_where = '',
    append=True,
)

extract_last_splcom_assessments_update_from_oracle_for_revenue = GeopetlReadOperator(
    task_id='read_last_splcom_assessments_update_from_oracle_for_revenue',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/last_splcom_assessments_update_timestamp.csv',
    db_conn_id='gisdbp_t_gis_opa',
    db_table_name='GIS_OPA.SPLCOM_ASSESSMENTS',
    db_table_where='',
    db_sql=get_last_asmt_update_from_oracle_for_revenue_stmt.format(table_name='gis_opa.splcom_assessments')
)

splcom_assessments_table_name = 'opa_test.splcom_assessments'
splcom_assessments_view_name = 'opa_test.vw_splcom_assessments'
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
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa_test', 'table_name': 'splcom_assessments', 'hash_field': 'etl_hash'},
)

update_splcom_assessments_history = PythonOperator(
    task_id='update_splcom_assessments_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa_test', 'table_name': 'splcom_assessments', 'hash_field': 'etl_hash'},
)

db2_splcom_assessments_updates_table_name='opa_test.splcom_assessments_history'
extract_splcom_assessments_updates_for_revenue = PythonOperator(
    task_id='read_splcom_assessments_updates_for_revenue',
    dag=pipeline,
    python_callable=extract_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_splcom_assessments_updates_table_name, 'stmt': get_updates_from_db2_for_revenue_stmt.format(table_name=db2_splcom_assessments_updates_table_name, fields=splcom_assessments_update_fields),
        'stmt_where': get_updates_from_db2_for_revenue_stmt_where},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/splcom_assessments_updates_for_revenue.csv', 'update_date_csv_path':'{{ ti.xcom_pull("make_staging") }}/last_splcom_assessments_update_timestamp.csv'},
)

write_splcom_assessments_updates_for_revenue_to_db_oracle = GeopetlWriteOperator(
    task_id='write_splcom_assessments_updates_for_revenue',
    dag=pipeline,
    csv_path = '{{ ti.xcom_pull("make_staging") }}/splcom_assessments_updates_for_revenue.csv',
    db_conn_id='gisdbp_t_gis_opa',
    db_table_name='GIS_OPA.SPLCOM_ASSESSMENTS',
    db_table_where = '',
    append=True,
)

delete_temp_splcom_assessments_updates = PythonOperator(
    task_id='delete_temp_splcom_assessments_updates',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'splcom_assessments_updates_for_revenue.csv',},
)

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

# update property assessments from cama & brtprod:
property_assessments_table_name = 'property_test.property_assessments'
property_assessments_view_name = 'property_test.vw_property_assessments_v3'
update_property_assessments = PythonOperator(
    task_id='update_property_assessments',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt': update_table_from_view_stmt.format(table_name=property_assessments_table_name,view_name=property_assessments_view_name)},
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
extract_phl_adrcrtval_vw.set_downstream(write_phl_adrcrtval_vw)
extract_splcom.set_downstream(write_splcom)
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
extract_asmt_cert_updates.set_upstream(write_phl_asmt)
extract_asmt_cert_updates.set_upstream(write_dweldat)
extract_asmt_cert_updates.set_upstream(write_pardat)
extract_asmt_cert_updates.set_upstream(write_exdet)
extract_asmt_cert_updates.set_upstream(write_owndat)
extract_asmt_cert_updates.set_downstream(write_asmt_cert_updates)
write_phl_adrcrtval_vw.set_downstream(update_phl_adrcrtval_vw_hash)
write_phl_adrcrtval_vw.set_downstream(delete_temp_phl_adrcrtval_vw)
write_splcom.set_downstream(update_splcom_hash)
write_splcom.set_downstream(delete_temp_splcom)
write_splcom.set_downstream(update_splcom_assessments)
write_pardat.set_downstream(update_pardat_hash)
write_pardat.set_downstream(delete_temp_pardat)
write_pardat.set_downstream(update_pardat_latest_prop_data)
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
write_phl_asmt.set_downstream(update_phl_asmt_hash)
write_phl_asmt.set_downstream(delete_temp_phl_asmt)
write_phl_acct_hist_det.set_downstream(update_phl_acct_hist_det_hash)
write_phl_acct_hist_det.set_downstream(delete_temp_phl_acct_hist_det)
write_asmt_cert_updates.set_downstream(update_asmt_cert_updates_hash)
update_asmt_cert_updates_hash.set_downstream(update_asmt_cert_updates_history)
update_asmt_cert_updates_history.set_downstream(extract_last_asmt_update_from_oracle_for_revenue)
extract_last_asmt_update_from_oracle_for_revenue.set_downstream(extract_asmt_updates_for_revenue)
extract_asmt_updates_for_revenue.set_downstream(write_asmt_updates_for_revenue_to_db_oracle)
write_asmt_updates_for_revenue_to_db_oracle.set_downstream(cleanup)
write_asmt_cert_updates.set_downstream(delete_temp_asmt_cert_updates)
write_asmt_cert_updates.set_downstream(update_property_assessments)
update_property_assessments.set_downstream(update_splcom_assessments)
update_splcom_assessments.set_downstream(update_splcom_assessments_hash)
update_splcom_assessments_hash.set_downstream(update_splcom_assessments_history)
update_splcom_assessments_history.set_downstream(extract_splcom_assessments_updates_for_revenue)
extract_splcom_assessments_updates_for_revenue.set_downstream(write_splcom_assessments_updates_for_revenue_to_db_oracle)
write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(delete_temp_splcom_assessments_updates)
write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(cleanup)
extract_last_splcom_assessments_update_from_oracle_for_revenue.set_upstream(make_staging)
extract_last_splcom_assessments_update_from_oracle_for_revenue.set_downstream(extract_splcom_assessments_updates_for_revenue)

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
update_phl_adrcrtval_vw_history.set_downstream(cleanup)
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
#update_asmt_cert_updates_history.set_downstream(extract_last_asmt_update_from_oracle_test_for_revenue)
#extract_last_asmt_update_from_oracle_test_for_revenue.set_downstream(extract_test_asmt_updates_for_revenue)
#extract_test_asmt_updates_for_revenue.set_downstream(write_test_asmt_updates_for_revenue_to_db_oracle)
#write_test_asmt_updates_for_revenue_to_db_oracle.set_downstream(cleanup)

#extract_splcom_updates_for_revenue.set_downstream(write_splcom_updates_for_revenue_to_db_oracle_test)
#write_splcom_updates_for_revenue_to_db_oracle_test.set_downstream(cleanup)

update_property_party.set_upstream(update_pardat_latest_prop_data)
update_property_party.set_upstream(write_owndat)
update_property_party.set_downstream(cleanup)


#extract_last_splcom_assessments_update_from_oracle_for_revenue.set_downstream(extract_splcom_assessments_updates_for_revenue)
#extract_splcom_assessments_updates_for_revenue.set_downstream(write_splcom_assessments_updates_for_revenue_to_db_oracle)
#write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(delete_temp_splcom_assessments_updates)
#write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(cleanup)

#update_splcom_assessments.set_downstream(extract_splcom_assessments_updates_for_revenue)
#extract_splcom_assessments_updates_for_revenue.set_downstream(write_splcom_assessments_updates_for_revenue_to_db_oracle)
#write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(delete_temp_splcom_assessments_updates)
#write_splcom_assessments_updates_for_revenue_to_db_oracle.set_downstream(cleanup)


