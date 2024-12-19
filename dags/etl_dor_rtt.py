
import os
import petl as etl
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
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

pipeline = DAG('etl_dor_rtt_geopetl_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG


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
    table_name=kwargs['table_name']
    version_name=kwargs.get('version_name', '')
    stmt=stmt if stmt else '''select * from {table_name}'''.format(table_name=table_name)
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    #pg_hook.bulk_dump(table_name, csv_path)
    version_stmt = "" if not version_name else "SELECT sde.sde_set_current_version('{}');".format(version_name)
    conn=pg_hook.get_conn()
    cur=conn.cursor()
    if version_stmt:
        cur.execute(version_stmt)
    export_stmt = '''COPY ({stmt}) to stdout with csv header;'''.format(stmt=stmt)
    with open(csv_path, 'w') as f_output:
        cur.copy_expert(export_stmt, f_output)
    conn.close()
    #pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_dor_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_databridge_rtt_summary = GeopetlReadOperator(
    task_id='read_databridge_rtt_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_rtt_summary.csv',
    db_conn_id='databridge',
    db_table_name='gis_dor.rtt_summary',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge-Raw

write_databridge_rtt_summary = GeopetlWriteOperator(
    task_id='write_databridge_rtt_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_dor_staging") }}/databridge_rtt_summary.csv',
    db_conn_id='databridge2',
    db_table_name='dor.databridge_rtt_summary',
)

#------------------------------------------------------------------
# Remove temp files

delete_temp_databridge_rtt_summary = PythonOperator(
    task_id='delete_temp_databridge_rtt_summary',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_dor_staging") }}', 'filename':'databridge_rtt_summary.csv',},
)

#------------------------------------------------------------------
# Update table dor.latest_processed_rtt_summary_deed from view

latest_processed_rtt_summary_deed_table_name = 'dor.latest_processed_rtt_summary_deed'
latest_processed_rtt_summary_deed_view_name = 'dor.vw_latest_processed_rtt_summary_deed'
update_latest_processed_rtt_summary_deed_stmt = '''
BEGIN;
truncate table {latest_processed_rtt_summary_deed_table_name};
insert into {latest_processed_rtt_summary_deed_table_name} (select * from {latest_processed_rtt_summary_deed_view_name});
COMMIT;
'''.format(latest_processed_rtt_summary_deed_table_name=latest_processed_rtt_summary_deed_table_name, latest_processed_rtt_summary_deed_view_name=latest_processed_rtt_summary_deed_view_name)

update_latest_processed_rtt_summary_deed = PythonOperator(
    task_id='update_latest_processed_rtt_summary_deed',
    dag=pipeline,
    python_callable=update_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_latest_processed_rtt_summary_deed_stmt},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_dor_staging") }}',
)

extract_databridge_rtt_summary.set_upstream(make_staging)
extract_databridge_rtt_summary.set_downstream(write_databridge_rtt_summary)
write_databridge_rtt_summary.set_downstream(update_latest_processed_rtt_summary_deed)
write_databridge_rtt_summary.set_downstream(delete_temp_databridge_rtt_summary)
update_latest_processed_rtt_summary_deed.set_downstream(cleanup)

