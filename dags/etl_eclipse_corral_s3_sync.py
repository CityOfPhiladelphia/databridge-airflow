from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
#from airflow.operators import PetlWriteOperator, PetlReadOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from datetime import datetime, timedelta
from airflow.models import Variable


# ============================================================
# Params:

eclipse_zoning_docs_schema = Variable.get('schemas') + 'db_eclipse_zoning_docs.json'

last_update_field = 'gisinfolastupdated'

db_table_name = 'lni.eclipse_corral_zoning_docs_s3_sync'

databridge_stmt = '''
select max({last_update_field}) as {last_update_field}
from {table_name}
'''.format(last_update_field=last_update_field, table_name=db_table_name)

eclipse_stmt = '''
select jobid, externalfilenum, issuedate, description, statusdescription, permitlocation, approveddevelopment, publicdownloadurl, addressobjectid, addresssourceobjectid, gisinfolastupdated, CURRENT_TIMESTAMP as etl_read_timestamp
from lmscorral.documents d
join lmscorral.zoningpermit z
on d.parentjobid = z.jobid
where d.publicdownloadurl is not null
'''

where_date = '''
and {last_update_field} > to_date('{min_date}', 'YYYY-MM-DD')
'''

last_sync_date = None

#ecplise_stmt_fmt = eclipse_stmt + where_date.format(last_update_field=last_update_field, min_date=min_date) if min_date else source_stmt


def get_last_s3_sync(**kwargs):
    global last_sync_date
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    cur = pg_hook.get_cursor()
    cur.execute(databridge_stmt)
    last_sync_date = cur.fetchone()[0]
    print("last sync date: ", last_sync_date)
    return last_sync_date

def get_new_doc_rows(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    cur = pg_hook.get_cursor()
    cur.execute(databridge_stmt)



# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 4, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_eclipse_corral_doc_s3_sync_v0', schedule_interval=None, default_args=default_args)

# ------------------------------------------------------------
# Make staging area

#make_staging = CreateStagingFolder(
#    task_id='make_eclipse_corral_doc_s3_sync_staging',
#    dag=pipeline,
#)

# get last s3 sync date:
get_last_s3_sync_date = PythonOperator(
    task_id='get_last_s3_sync_date',
    python_callable=get_last_s3_sync,
    dag=pipeline,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':databridge_stmt},
)

# get doc rows since last sync:
#get_new_doc_rows = PetlReadOperator(
#    task_id='get_new_doc_rows',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_eclipse_corral_doc_s3_sync_staging") }}/new_doc_rows.csv',
#    db_conn_id='eclipse-corral',
##    db_sql=eclipse_stmt + where_date.format(last_update_field=last_update_field, min_date=last_sync_date) if last_sync_date else eclipse_stmt,
#    db_sql=eclipse_stmt,
#    sql_override=True,
#)

# update s3 with new docs:

# update databridge

# read from databridge
#extract_eclipse_zoning_docs = GeopetlReadOperator(
#    task_id='read_db_eclipse_zoning_docs',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_db_eclipse_corral_doc_s3_sync_staging") }}/eclipse_zoning_docs.csv',
#    db_conn_id='databridge2',
#    db_table_name='lni.eclipse_zoning_docs',
#    db_table_where='',
#    db_timestamp=False,
#)

# update carto
#update_carto = CartoUpdateOperator(
#    task_id='update_carto',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_eclipse_corral_doc_s3_sync_staging") }}/eclipse_zoning_docs.csv',
#    db_conn_id='carto_phl',
#    db_table_name='eclipse_zoning_docs',
#    db_schema_json=eclipse_zoning_docs_schema,
#    db_select_users=['publicuser', 'tileuser']
#)


# Cleanup - delete staging folder
#cleanup = DestroyStagingFolder(
#    task_id='cleanup_staging',
#    dag=pipeline,
#    dir='{{ ti.xcom_pull("make_eclipse_corral_doc_s3_sync_staging") }}',
#)

