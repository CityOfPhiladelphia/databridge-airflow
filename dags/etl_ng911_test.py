from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator, GeopetlWriteOperatorDev
from airflow.operators.python_operator import PythonOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta
import psycopg2

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_ng911_test_geopetl_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG


# ------------------------------------------------------------
# Utils

def update_postgres(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_ng911_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_ng911_siteaddresses_guids = GeopetlReadOperator(
    task_id='read_ng911_siteaddresses_guids',
    csv_path='{{ ti.xcom_pull("make_ng911_staging") }}/ng911_site_addresses_guids.csv',
    dag=pipeline,
    db_conn_id='ng911',
    db_table_name='ng911_siteaddresses',
    db_sql='select guid, status from ng911_siteaddresses', 
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_ng911_siteaddresses_guids = GeopetlWriteOperator(
    task_id='write_ng911_siteaddresses_guids',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_ng911_staging") }}/ng911_site_addresses_guids.csv',
    db_conn_id='databridge2',
    db_table_name='ng911.guids',
)

# -----------------------------------------------------------------
# Extract new records for ng911
extract_new_ng911_site_addresses = GeopetlReadOperator(
    task_id='read_new_ng911_site_addresses',
    csv_path='{{ ti.xcom_pull("make_ng911_staging") }}/new_ng911_site_addresses.csv',
    dag=pipeline,
    db_conn_id='databridge2',
    db_table_name='ng911.vw_new_ng911_rows',
    db_sql='''select 
                guid
                ,status
                ,flag
                ,comment
                ,place_type
                ,placement
                ,street_address
                ,address
                ,address_suffix
                ,address_fractional
                ,address_high
                ,street_predir
                ,street_name
                ,street_suffix
                ,street_postdir
                ,building
                ,floor
                ,unit_type
                ,unit_num
                ,zip_code
                ,zip_4
                ,addtl_loc
                ,landmkname
                ,mile_post
                ,geocode_type
                ,street_code
                ,seg_id
                ,seg_side
                ,date_expire
                ,geometry_flag
                ,date_update
                ,updater
                ,date_effective
                ,creator
                ,st_astext(shape) as shape
                ,zip_10
                ,streetname_long
                ,munic_code
                ,county_code
                ,msagcomm
                ,gc_exception
                from ng911.vw_new_ng911_rows''',
)

# Extract records for updating ng911 geom
extract_geom_updates_for_ng911_site_addresses = GeopetlReadOperator(
    task_id='read_geom_updates_for_ng911_site_addresses',
    csv_path='{{ ti.xcom_pull("make_ng911_staging") }}/geom_updates_for_ng911_site_addresses.csv',
    dag=pipeline,
    db_conn_id='databridge2',
    db_table_name='ng911.vw_address_point_updates',
    db_sql='select guid, geocode_type, st_astext(shape) as shape from ng911.vw_address_point_updates', 
)

# -----------------------------------------------------------------
# Update ng911 site addresses

# Append new ng911 site addresses
append_new_ng911_site_addresses = GeopetlWriteOperatorDev(
    task_id='append_new_ng911_site_addresses',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_ng911_staging") }}/new_ng911_site_addresses.csv',
    db_conn_id='ng911-test',
    db_table_name='ng911.ng911_siteaddresses',
    append=True,
)

# Update geom for site_addresses where status in (2,3,4)
def update_ng911_site_addresses(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    source_csv=templates_dict['csv_path']
    params = BaseHook.get_connection(db_conn_id)
    conn = psycopg2.connect(dbname=params.schema, user=params.login, password=params.password, host=params.host, port=params.port)
    try:
        with conn.cursor() as cur:
            cur.execute("""CREATE TEMPORARY TABLE NG911_SITEADDRESSES_GEOM_UPDATES_STAGING ( 
                GUID TEXT,
                GEOCODE_TYPE TEXT,
                SHAPE TEXT
              )
                        ON COMMIT DROP""")

            with open(source_csv) as data:
                cur.copy_expert("""COPY NG911_SITEADDRESSES_GEOM_UPDATES_STAGING ( guid, geocode_type, shape )
                                FROM STDIN WITH CSV""", data)

            cur.execute("""UPDATE NG911.NG911_SITEADDRESSES prod
            SET geocode_type = staging.geocode_type,
                shape = case when staging.shape is not null then ST_GEOMETRY(staging.shape, 2272) else null end
                from   NG911_SITEADDRESSES_GEOM_UPDATES_STAGING staging
                where staging.guid = prod.guid
            """)

    except:
        conn.rollback()
        raise

    else:
        conn.commit()

    finally:
        conn.close()

update_geom_for_ng911_site_addresses = PythonOperator(
    task_id='update_geom_for_ng911_site_addresses',
    dag=pipeline,
    python_callable=update_ng911_site_addresses,
    provide_context=True,
    op_kwargs={'db_conn_id':'ng911-test'},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_ng911_staging") }}/geom_updates_for_ng911_site_addresses.csv'},
)



# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_ng911_staging") }}',
)

extract_ng911_siteaddresses_guids.set_upstream(make_staging)
extract_ng911_siteaddresses_guids.set_downstream(write_ng911_siteaddresses_guids)
write_ng911_siteaddresses_guids.set_downstream(extract_new_ng911_site_addresses)
write_ng911_siteaddresses_guids.set_downstream(extract_geom_updates_for_ng911_site_addresses)
extract_new_ng911_site_addresses.set_downstream(append_new_ng911_site_addresses)
extract_geom_updates_for_ng911_site_addresses.set_downstream(update_geom_for_ng911_site_addresses)
append_new_ng911_site_addresses.set_downstream(cleanup)
update_geom_for_ng911_site_addresses.set_downstream(cleanup)
