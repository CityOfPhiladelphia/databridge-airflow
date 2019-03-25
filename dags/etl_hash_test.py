from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.slack import slack_failed_alert, slack_success_alert
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
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_hash_test_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ----------------------------------------------------
# Update hash
table_schema='opa'
table_name='buildingcodes'
hash_field='etl_hash'
def update_hash_fields(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='databridge2')
    hash_fields_stmt = '''
        SELECT array_agg(COLUMN_NAME::text order by COLUMN_NAME)
        FROM information_schema.columns
        WHERE table_schema='{table_schema}' AND table_name='{table_name}'
        and column_name not like 'etl%'
    '''.format(table_schema=table_schema, table_name=table_name)
    hash_fields = pg_hook.get_records(hash_fields_stmt)[0][0]
    print(hash_fields)
    hash_calc = 'md5(' + ' || '.join(hash_fields) + ')::uuid'
    print(hash_calc)
    update_stmt = '''
    update {table_name_full} set {hash_field} = {hash_calc}
    '''.format(table_name_full='.'.join([table_schema, table_name]), hash_field=hash_field, hash_calc=hash_calc)
    pg_hook.run(update_stmt)


update_hash = PythonOperator(
    task_id='update_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
)


update_hash
