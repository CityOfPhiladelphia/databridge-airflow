import os
import petl as etl
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperatorDev, GeopetlWriteOperator
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

pipeline = DAG('etl_named_version_geopetl_test', schedule_interval=None, default_args=default_args)  # TODO: Lo$


def delete_temp_file(**kwargs):
    path = kwargs['templates_dict']['csv_path']
    filename = kwargs['templates_dict']['filename']
    os.remove(path + '/' + filename)


make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)


extract_tripoli_dor_parcel = GeopetlReadOperatorDev(
    task_id='read_tripoli_dor_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/tripoli_dor_parcel.csv',
    db_conn_id='tripoli_dor',
    db_table_name='dor.parcel_evw0',
    db_sql='''select objectid,recsub,basereg,mapreg,parcel,recmap,stcod,house,suf,unit,stex,stdir,stnam,stdessuf,elev_flag,topelev,botelev,condoflag,matchflag,
            inactdate,orig_date,status,geoid,stdes,addr_source,addr_std,created_user,created_date,last_edited_user,last_edited_date,last_edit_reason,
            axiomaticid,comments,oid_bu,case when r = 1 then pin else Null end as pin,frac,unit_type,stex_frac,stex_suf,separated_rights,dor_review,opa_review,pwd_review,muniment_type,muniment_id,
            title,etl_read_timestamp,shape::text
            from(
            select objectid,recsub,basereg,mapreg,parcel,recmap,stcod,house,suf,unit,stex,stdir,stnam,stdessuf,elev_flag,topelev,botelev,condoflag,matchflag,
            inactdate,orig_date,status,geoid,stdes,addr_source,addr_std,created_user,created_date,last_edited_user,last_edited_date,last_edit_reason,'' as axiomaticid,
            comments,'' as oid_bu,pin,frac,unit_type,stex_frac,stex_suf,separated_rights,dor_review,opa_review,pwd_review,muniment_type,muniment_id,'' as title,
            current_timestamp as etl_read_timestamp,st_astext(shape) as shape, row_number() over(partition by pin order by last_edited_date nulls last) as r
            from dor.parcel_evw0 where st_isempty(shape) is false and status in (1,3)
            ) prep
            union
            select objectid,recsub,basereg,mapreg,parcel,recmap,stcod,house,suf,unit,stex,stdir,stnam,stdessuf,elev_flag,topelev,botelev,condoflag,matchflag,
            inactdate,orig_date,status,geoid,stdes,addr_source,addr_std,created_user,created_date,last_edited_user,last_edited_date,last_edit_reason,'' as axiomaticid,
            comments, '' as oid_bu,Null as pin,frac,unit_type,stex_frac,stex_suf,separated_rights,dor_review,opa_review,pwd_review,muniment_type,muniment_id,'' as title,
            current_timestamp as etl_read_timestamp,st_astext(shape)::text as shape
            from dor.parcel_evw0 where st_isempty(shape) is false and status not in (1,3)''',
#    db_table_name='dor.parcel',
#    db_table_where='st_isempty(shape) is false',
    named_version='QAQC',
)

write_tripoli_dor_parcel = GeopetlWriteOperator(
    task_id='write_tripoli_dor_parcel',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/tripoli_dor_parcel.csv',
    db_conn_id='databridge2',
    db_table_name='dor.tripoli_dor_parcel',
)

delete_temp_tripoli_dor_parcel = PythonOperator(
    task_id='delete_temp_tripoli_dor_parcel',
    dag=pipeline,
    python_callable=delete_temp_file,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}', 'filename':'tripoli_dor_parcel.csv',},
)

update_tripoli_dor_parcel_hash = PythonOperator(
    task_id='update_tripoli_dor_parcel_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_parcel', 'hash_field': 'etl_hash'},
)


update_tripoli_dor_parcel_history = PythonOperator(
    task_id='update_tripoli_dor_parcel_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'tripoli_dor_parcel', 'hash_field': 'etl_hash'},
)


cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)


extract_tripoli_dor_parcel.set_upstream(make_staging)
extract_tripoli_dor_parcel.set_downstream(write_tripoli_dor_parcel)
write_tripoli_dor_parcel.set_downstream(update_tripoli_dor_parcel_hash)
write_tripoli_dor_parcel.set_downstream(delete_temp_tripoli_dor_parcel)
update_tripoli_dor_parcel_hash.set_downstream(update_tripoli_dor_parcel_history)
update_tripoli_dor_parcel_history.set_downstream(cleanup)
