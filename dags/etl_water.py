from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from datetime import datetime, timedelta


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 4, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_water_geopetl_v0', schedule_interval='0 7 * * *', default_args=default_args)

def export_table_from_postgres(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    table_name=kwargs['table_name']
    stmt=stmt if stmt else '''select * from {table_name}'''.format(table_name=table_name)
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    #pg_hook.bulk_dump(table_name, csv_path)
    pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)


# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_water_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from Databridge

#extract_pwd_parcels = GeopetlReadOperator(
#    task_id='read_pwd_parcels',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_water_staging") }}/pwd_parcels.csv',
#    db_conn_id='databridge',
#    db_table_name='gis_water.pwd_parcels',
#    db_table_where='',
#)
#
## ----------------------------------------------------
## Write extracted files to Databridge-Raw
#write_pwd_parcels = GeopetlWriteOperator(
#    task_id='write_pwd_parcels',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_water_staging") }}/pwd_parcels.csv',
#    db_conn_id='databridge2',
#    db_table_name='water.databridge_pwd_parcels',
#)
#
## Update hash
#update_pwd_parcels_hash = PythonOperator(
#    task_id='update_pwd_parcels_hash',
#    dag=pipeline,
#    python_callable=update_hash_fields,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'water', 'table_name': 'databridge_pwd_parcels', 'hash_field': 'etl_hash'},
#)
#
## Update history
#update_pwd_parcels_history = PythonOperator(
#    task_id='update_pwd_parcels_history',
#    dag=pipeline,
#    python_callable=update_history_table,
#    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'water', 'table_name': 'databridge_pwd_parcels', 'hash_field': 'etl_hash'},
#)
# -----------------------------------------------------------------
# Extract water.vw_databridge_pwd_parcels_pinned from Databridge-Raw (this is temporary until PWD integrates PIN)
extract_pinned_pwd_parcels = GeopetlReadOperator(
    task_id='read_pinned_pwd_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_water_staging") }}/pinned_pwd_parcels.csv',
    db_conn_id='databridge2',
    db_table_name='water.vw_databridge_pwd_parcels_pinned',
    db_sql='select parcelid, tencode, address, owner1, owner2, bldg_code, bldg_desc, brt_id, num_brt, num_accounts, gross_area, pin, st_astext(shape) as shape from water.vw_databridge_pwd_parcels_pinned',
)


#extract_pinned_pwd_parcels = PythonOperator(
#    task_id='read_pinned_pwd_parcels',
#    dag=pipeline,
#    python_callable=export_table_from_postgres,
#    provide_context=True,
#    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_pinned_pwd_parcels_view_name, 'stmt': read_pinned_pwd_parcels_stmt},
#    templates_dict={'csv_path':'{{ ti.xcom_pull("make_water_staging") }}/pinned_pwd_parcels.csv'},
#)

# -----------------------------------------------------------------
# Write pinned pwd parcels to Databridge

write_pinned_pwd_parcels = GeopetlWriteOperator(
    task_id='write_pinned_pwd_parcels',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_water_staging") }}/pinned_pwd_parcels.csv',
    db_conn_id='databridge-water',
    db_table_name='gis_water.pwd_parcels_pinned',
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_water_staging") }}',
)

extract_pinned_pwd_parcels.set_upstream(make_staging)
extract_pinned_pwd_parcels.set_downstream(write_pinned_pwd_parcels)
write_pinned_pwd_parcels.set_downstream(cleanup)


#extract_pwd_parcels.set_upstream(make_staging)
#extract_pwd_parcels.set_downstream(write_pwd_parcels)
#write_pwd_parcels.set_downstream(update_pwd_parcels_hash)
#update_pwd_parcels_hash.set_downstream(update_pwd_parcels_history)
#update_pwd_parcels_history.set_downstream(cleanup)
#extract_pinned_pwd_parcels.set_upstream(write_pwd_parcels)
#extract_pinned_pwd_parcels.set_downstream(write_pinned_pwd_parcels)
#write_pinned_pwd_parcels.set_downstream(cleanup)
