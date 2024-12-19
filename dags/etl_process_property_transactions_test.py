import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator, GeopetlUpsertOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
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
    'on_failure_callback': slack_failed_alert,
    'start_date': datetime(2020, 11, 10, 0, 0, 0),
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_process_property_transactions_test_v0', schedule_interval=None, default_args=default_args)

def update_db(**kwargs):
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


# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# update pin master:
pin_master_table_name='property_test.pin_master'
pin_master_view_name='property_test.vw_pin_master'
pin_master_temp_table_name='temp_pin_master'
upsert_pin_master_stmt = upsert_pin_master_sql.format(pin_master_table_schema_name=pin_master_table_name, pin_master_view_schema_name=pin_master_view_name)
pin_master_update_stmt = '''
BEGIN;
create temp table {pin_master_temp_table_name} as select * from {pin_master_view_name};
CREATE INDEX {pin_master_temp_table_name}_idx
    ON {pin_master_temp_table_name} USING btree
    (pin);
{upsert_stmt}
delete from {pin_master_table_name} main where pin in (
	select pin from {pin_master_table_name}
	except
	select  pin from {pin_master_temp_table_name}
);
COMMIT;
'''.format(pin_master_table_name=pin_master_table_name, pin_master_view_name=pin_master_view_name, pin_master_temp_table_name=pin_master_temp_table_name, upsert_stmt=upsert_pin_master_stmt)

update_pin_master = PythonOperator(
    task_id='update_pin_master',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':pin_master_update_stmt},
)

# update stage records:
upsert_records_into_er_stage_transaction = PythonOperator(
    task_id='upsert_records_into_er_stage_transaction',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_er_stage_transaction_sql},
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
    bash_command='''batch_geocoder ais --input-file '{{ ti.xcom_pull("make_staging") }}/er_stage_props_addresses_for_geodcoding.csv' --ais-url http://api.phila.gov/ais/v1/ --query-fields concatenated_address --ais-fields normalized,match_type,street_address,address_low,address_low_suffix,address_low_frac,address_high,street_predir,street_name,street_suffix,street_postdir,unit_type,unit_num,street_code,seg_id,zip_code,zip_4,usps_bldgfirm,usps_type,opa_account_num,dor_parcel_id,pwd_parcel_id,bin,li_parcel_id,li_address_key,eclipse_location_id,geocode_type,shape  --ais-key 5e0ee5872bd78c3b01b7913ab5083dc9 > '{{ ti.xcom_pull("make_staging") }}/geocoded_er_stage_props.csv' ''',
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

er_stage_parcel_pin_matching_table_name = 'property_test.er_stage_parcel_pin_matching'
er_stage_parcel_pin_matching_view_name = 'property_test.vw_er_stage_parcel_pin_matching'
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

property_deeds_new_table_name = 'cama_test.property_deeds_new'
property_deeds_new_view_name = 'cama_test.vw_property_deeds_new'
upsert_property_deeds_new_stmt = upsert_property_deeds_new_sql.format(property_deeds_new_table_schema_name=property_deeds_new_table_name, property_deeds_view_schema_name=property_deeds_new_view_name)
update_cama_property_deeds_new = PythonOperator(
    task_id='update_cama_property_deeds_new',
    dag=pipeline,
    python_callable=update_db,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':upsert_property_deeds_new_stmt},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

make_staging.set_downstream(update_pin_master)
update_pin_master.set_downstream(upsert_records_into_er_stage_transaction)
update_pin_master.set_downstream(upsert_records_into_er_stage_parcel)
update_pin_master.set_downstream(upsert_records_into_er_stage_parties)
upsert_records_into_er_stage_transaction.set_downstream(update_er_stage_parcel_pin_matching)
upsert_records_into_er_stage_parcel.set_downstream(extract_er_stage_props_for_geocoding)
upsert_records_into_er_stage_parties.set_downstream(update_er_stage_parcel_pin_matching)
extract_er_stage_props_for_geocoding.set_downstream(geocode_er_stage_props)
geocode_er_stage_props.set_downstream(upsert_geocoded_er_stage_props)
upsert_geocoded_er_stage_props.set_downstream(update_er_stage_parcel_pin_matching)
update_er_stage_parcel_pin_matching.set_downstream(update_cama_property_deeds_new)
update_cama_property_deeds_new.set_downstream(cleanup)

