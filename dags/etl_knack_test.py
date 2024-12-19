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
from airflow.utils.campfin_etl import run_etl
from airflow.utils.campfin_transactions_etl import etl_transactions_from_tripoli_to_db
from airflow.utils.campfin_load_csvs import etl_load_csvs
from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators import SSHOperator
from airflow.hooks.base_hook import BaseHook
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
    'start_date': datetime(2023, 1, 26, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_knack_test_v0', schedule_interval=None, default_args=default_args)

def write_to_postgres(templates_dict, **kwargs):

    db_conn_id=kwargs['db_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    stmt=kwargs.get('stmt', '')
    table_name=kwargs['table_name']
    csv_path=templates_dict['csv_path']
    temp_csv_path = csv_path.replace('csv', '_temp.csv')

    # Get target table columns:
    target_columns_stmt = f'''select column_name from information_schema.columns where table_name = '{table_name}' '''
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.execute(target_columns_stmt)
    target_columns = [f[0] for f in cur.fetchall()]
    conn.close()

    # Create string header:
    rows = etl.fromcsv(csv_path, encoding='utf-8') 
    header = rows[0]
    str_header = ''
    header_cols_in_table = [f for f in header if f in target_columns]
    num_fields = len(header_cols_in_table)
    for i, field in enumerate(header_cols_in_table):
        if i < num_fields - 1:
            str_header += field + ', '
        else:
            str_header += field
    print(str_header)

    # Check if target table header and csv have same columns:
    header_sorted = sorted(header)
    target_columns_sorted = sorted(target_columns)
    if header_sorted != target_columns_sorted:
        # If different, output temp csv with target table columns only:
        rows.cut(header_cols_in_table).tocsv(temp_csv_path, encoding='utf-8')
        csv_path = temp_csv_path
    stmt_fmt = stmt.format(header=str_header, table_name=table_name)
    pg_hook.copy_expert(stmt_fmt, csv_path)

#
# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

#
# --------------------------------------------------------------
# Extract candidates csv from Knack
knack_campfin_creds = BaseHook.get_connection('knack-campfin')
app_id = knack_campfin_creds.login
api_key = knack_campfin_creds.password
candidates_knack_object_id = 1
campaigns_knack_object_id = 2
filer_types_knack_object_id = 3


extract_knack_candidates = BashOperator(
    task_id='extract_knack_candidates',
    bash_command=f'''extract-knack extract-records {app_id} {api_key} {candidates_knack_object_id} ''' + ''' > '{{ ti.xcom_pull("make_staging") }}/knack_candidates.csv' ''',
    dag=pipeline
)

extract_knack_campaigns = BashOperator(
    task_id='extract_knack_campaigns',
    bash_command=f'''extract-knack extract-records {app_id} {api_key} {campaigns_knack_object_id} ''' + ''' > '{{ ti.xcom_pull("make_staging") }}/knack_campaigns.csv' ''',
    dag=pipeline
)

extract_knack_filer_types = BashOperator(
    task_id='extract_knack_filer_types',
    bash_command=f'''extract-knack extract-records {app_id} {api_key} {filer_types_knack_object_id} ''' + ''' > '{{ ti.xcom_pull("make_staging") }}/knack_filer_types.csv' ''',
    dag=pipeline
)


#---------------------------------------------------------------
# Write to Databridge:
write_csv_to_postgres_stmt = '''
BEGIN;
truncate table {table_name};
COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true);
COMMIT;
'''
pg_knack_campaigns_table_name = 'campaigns_csv'
write_knack_campaigns = PythonOperator(
    task_id='write_knack_campaigns_to_pg',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'campfin', 'table_name': pg_knack_campaigns_table_name, 'stmt': write_csv_to_postgres_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/knack_campaigns.csv'},
)

pg_knack_candidates_table_name = 'candidates_csv'
write_knack_candidates = PythonOperator(
    task_id='write_knack_candidates_to_pg',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'campfin', 'table_name': pg_knack_candidates_table_name, 'stmt': write_csv_to_postgres_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/knack_candidates.csv'},
)

pg_knack_filer_types_table_name = 'filer_types_csv'
write_knack_filer_types = PythonOperator(
    task_id='write_knack_filer_types_to_pg',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'campfin', 'table_name': pg_knack_filer_types_table_name, 'stmt': write_csv_to_postgres_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/knack_filer_types.csv'},
)

# extract_campaigns_csv = GeopetlReadOperator(
#     task_id='extract_campaigns',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_staging") }}/campaigns_csv.csv',
#     db_conn_id='campfin',
#     db_table_name='campaign_finance_opd.campaigns_csv',
#     db_table_where='',
# )


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)


make_staging >> extract_knack_candidates >> write_knack_candidates >> cleanup
make_staging >> extract_knack_campaigns >> write_knack_campaigns >> cleanup
make_staging >> extract_knack_filer_types >> write_knack_filer_types >> cleanup

