
import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
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

pipeline = DAG('etl_dor_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG


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

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_dor_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

#extract_databridge_rtt_summary = GeopetlReadOperator(
#    task_id='read_databridge_rtt_summary',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_rtt_summary.csv',
#    db_conn_id='databridge',
#    db_table_name='gis_dor.rtt_summary',
#    db_table_where='',
#)
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

extract_tripoli_dor_parcel = GeopetlReadOperator(
    task_id='read_tripoli_dor_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcel.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcel',
    db_table_where='st_isempty(shape) is false',
)

#extract_tripoli_dor_parcel_edits = GeopetlReadOperator(
#    task_id='read_tripoli_dor_parcel_edits',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcel_edits.csv',
#    db_conn_id='tripoli_dor',
#    db_table_name='dor.parcel_edits',
#    db_table_where='st_isempty(shape) is false',
#)

extract_tripoli_dor_pin_changes = GeopetlReadOperator(
    task_id='read_tripoli_dor_pin_changes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_pin_changes.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcel_cleanup_pin_edits',
    db_table_where='',
)

extract_tripoli_dor_pin_queue_review = GeopetlReadOperator(
    task_id='read_tripoli_dor_pin_queue_review',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_pin_queue_review.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.pin_queue_review',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge

#write_databridge_rtt_summary = GeopetlWriteOperator(
#    task_id='write_databridge_rtt_summary',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_rtt_summary.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.databridge_rtt_summary',
#)
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

write_tripoli_dor_parcel = GeopetlWriteOperator(
    task_id='write_tripoli_dor_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcel.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_dor_parcel',
)

#write_tripoli_dor_parcel_edits = GeopetlWriteOperator(
#    task_id='write_tripoli_dor_parcel_edits',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_parcel_edits.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.tripoli_dor_parcel_edits',
#)

write_tripoli_dor_pin_changes = GeopetlWriteOperator(
    task_id='write_tripoli_dor_pin_changes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_pin_changes.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_dor_pin_changes',
)

write_tripoli_dor_pin_queue_review = GeopetlWriteOperator(
    task_id='write_tripoli_dor_pin_queue_review',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/tripoli_dor_pin_queue_review.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_dor_pin_queue_review',
)

#------------------------------------------------------------------
# Remove temp files
delete_temp_tripoli_dor_parcel = PythonOperator(
    task_id='delete_temp_tripoli_dor_parcel',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_dor_parcel.csv',},
)

#delete_temp_tripoli_dor_parcel_edits = PythonOperator(
#    task_id='delete_temp_tripoli_dor_parcel_edits',
#    dag=pipeline,
#    python_callable=delete_temp_file,
#    provide_context=True,
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_dor_parcel_edits.csv',},
#)

delete_temp_tripoli_dor_pin_changes = PythonOperator(
    task_id='delete_temp_tripoli_dor_pin_changes',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_dor_pin_changes.csv',},
)

delete_temp_tripoli_dor_pin_queue_review = PythonOperator(
    task_id='delete_temp_tripoli_dor_pin_queue_review',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'tripoli_dor_pin_queue_review.csv',},
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

#update_tripoli_dor_parcel_edits_hash = PythonOperator(
#    task_id='update_tripoli_dor_parcel_edits_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_parcel_edits', 'hash_field': 'etl_hash'},
#)

update_tripoli_dor_pin_changes_hash = PythonOperator(
    task_id='update_tripoli_dor_pin_changes_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_pin_changes', 'hash_field': 'etl_hash'},
)

update_tripoli_dor_pin_queue_review_hash = PythonOperator(
    task_id='update_tripoli_dor_pin_queue_review_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_pin_queue_review', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories
#
update_tripoli_dor_parcel_history = PythonOperator(
    task_id='update_tripoli_dor_parcel_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_parcel', 'hash_field': 'etl_hash'},
)

#update_tripoli_dor_parcel_edits_history = PythonOperator(
#    task_id='update_tripoli_dor_parcel_edits_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_parcel_edits', 'hash_field': 'etl_hash'},
#)

update_tripoli_dor_pin_changes_history = PythonOperator(
    task_id='update_tripoli_dor_pin_changes_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_pin_changes', 'hash_field': 'etl_hash'},
)

update_tripoli_dor_pin_queue_review_history = PythonOperator(
    task_id='update_tripoli_dor_pin_queue_review_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_pin_queue_review', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Trigger dor-pwd-overlap-analysis
db2_parcel_overlap_analysis_view_name='dor.vw_pinned_dor_pwd_overlap_analysis'
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

# -----------------------------------------------------------------
# Estract and perform standardization report for distinct street_addresses from pin_source_addresses
pin_source_address_table_name = 'property.pin_source_address_2021'
pin_source_address_view_name = 'property.vw_pin_source_address_2021'

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
select distinct street_address from property.pin_source_address_2021
'''

extract_distinct_pin_source_addresses = GeopetlReadOperator(
    task_id='read_distinct_pin_source_addresses',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/distinct_pin_source_addresses.csv',
    db_conn_id='databridge2',
    db_table_name='property.pin_source_address_2021',
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

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_dor_staging") }}',
)

#extract_databridge_rtt_summary.set_upstream(make_staging)
#extract_databridge_rtt_summary.set_downstream(write_databridge_rtt_summary)
#write_rtt_summary.set_downstream(cleanup)
#
#extract_databridge_dor_parcel.set_upstream(make_staging)
#extract_databridge_dor_parcel.set_downstream(write_databridge_dor_parcel)
#write_databridge_dor_parcel.set_downstream(cleanup)
#
#extract_databridge_dor_easement.set_upstream(make_staging)
#extract_databridge_dor_easement.set_downstream(write_databridge_dor_easement)
#write_databridge_dor_easement.set_downstream(cleanup)

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
write_std_pin_source_address_comps.set_downstream(cleanup)
update_dor_pwd_overlap_analysis.set_downstream(cleanup)

#extract_tripoli_dor_parcel_edits.set_upstream(make_staging)
#extract_tripoli_dor_parcel_edits.set_downstream(write_tripoli_dor_parcel_edits)
#write_tripoli_dor_parcel_edits.set_downstream(update_tripoli_dor_parcel_edits_hash)
#write_tripoli_dor_parcel_edits.set_downstream(delete_temp_tripoli_dor_parcel_edits)
#update_tripoli_dor_parcel_edits_hash.set_downstream(update_tripoli_dor_parcel_edits_history)
#update_tripoli_dor_parcel_edits_history.set_downstream(cleanup)

extract_tripoli_dor_pin_changes.set_upstream(make_staging)
extract_tripoli_dor_pin_changes.set_downstream(write_tripoli_dor_pin_changes)
write_tripoli_dor_pin_changes.set_downstream(update_tripoli_dor_pin_changes_hash)
write_tripoli_dor_pin_changes.set_downstream(delete_temp_tripoli_dor_pin_changes)
update_tripoli_dor_pin_changes_hash.set_downstream(update_tripoli_dor_pin_changes_history)
update_tripoli_dor_pin_changes_history.set_downstream(cleanup)

extract_tripoli_dor_pin_queue_review.set_upstream(make_staging)
extract_tripoli_dor_pin_queue_review.set_downstream(write_tripoli_dor_pin_queue_review)
write_tripoli_dor_pin_queue_review.set_downstream(update_tripoli_dor_pin_queue_review_hash)
write_tripoli_dor_pin_queue_review.set_downstream(delete_temp_tripoli_dor_pin_queue_review)
update_tripoli_dor_pin_queue_review_hash.set_downstream(update_tripoli_dor_pin_queue_review_history)
update_tripoli_dor_pin_queue_review_history.set_downstream(cleanup)

