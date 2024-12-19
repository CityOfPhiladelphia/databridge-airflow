import pyodbc
import csv
import petl as etl
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.doroem_parcel_report1 import check_address_comps
from datetime import datetime, timedelta
from airflow.models import Variable

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 23, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_parcel_editing_v3', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG

eee_parcel_edits_table_name='dor.parcel'
eee_parcel_edits_analysis_table_name='dor.parcel_edits_analysis'
db2_parcel_edits_std_table_name='dor.parcel_edits_std'
db2_parcel_edits_analysis_view_name = 'dor.vw_parcel_edits_analysis'
db2_parcel_edits_analysis_table_name='dor.parcel_edits_analysis'
db2_parcel_overlap_analysis_view_name='dor.vw_dor_pwd_overlap_analysis'
db2_parcel_overlap_analysis_table_name='dor.dor_pwd_overlap_analysis'
temp_parcel_csv_name = 'parcel_edits.csv'
temp_parcel_std_csv_name = 'parcel_edits_std.csv'
temp_parcel_analysis_csv_name = 'parcel_edits_analysis.csv'

read_parcel_edits_sql = '''
select 
objectid
,recsub
,basereg
,mapreg
,parcel
,recmap
,stcod
,house
,suf
,frac
,unit_type
,unit
,stex
,stdir
,stnam
,stdessuf
,elev_flag
,topelev
,botelev
,condoflag
,matchflag
,inactdate
,orig_date
,status
,geoid
,stdes
,addr_source
,addr_std
,created_user
,created_date
,last_edited_user
,last_edited_date
,last_edit_reason
,axiomaticid
,comments
,citygeoedit
,citygeocomments
,st_geomfromtext(st_astext(shape)::text, 2272) as shape
,current_timestamp as etl_read_timestamp
from {table_name}
'''.format(table_name=eee_parcel_edits_table_name)

read_parcel_edits_analysis_stmt = '''
    select
    objectid
,concatenated_address
,std_address_low
,std_address_low_suffix
,std_address_low_frac
,std_address_high
,std_street_predir
,std_street_name
,std_street_suffix
,std_street_postdir
,std_unit_type
,std_unit_num
,std_street_address
,std_street_code
,std_seg_id
,cl_addr_match
,change_stcod
,change_house
,change_suf
,change_frac
,change_unit_type
,change_unit
,change_stex
,change_stdir
,change_stnam
,change_stdes
,change_stdessuf
,no_address
,no_mapreg
,num_parcels_w_address
,num_parcels_w_mapreg
,intersecting_seg_id
,research_tags
,match_type
,in_easement
,in_row
,curb_id
,etl_read_timestamp
,etl_modified_timestamp
    from {table_name}
'''.format(table_name=db2_parcel_edits_analysis_table_name)



#read_parcel_editing_stmt = '''
#COPY {sql} TO STDOUT with csv header;
#'''.format(sql=read_parcel_edits_sql)

write_parcel_edits_std_stmt = '''
BEGIN;
truncate table {table_name};
COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true);
COMMIT;
'''

update_dor_pwd_overlap_analysis_stmt = '''
BEGIN;
truncate table {parcel_overlap_analysis_table_name};
insert into {parcel_overlap_analysis_table_name} (select * from {parcel_overlap_analysis_view_name});
COMMIT;
'''.format(parcel_overlap_analysis_table_name=db2_parcel_overlap_analysis_table_name, parcel_overlap_analysis_view_name=db2_parcel_overlap_analysis_view_name)


update_db2_parcel_edits_analysis_stmt = '''
BEGIN;
truncate table {db2_parcel_edits_analysis_table_name};
insert into {db2_parcel_edits_analysis_table_name} (select * from {db2_parcel_edits_analysis_view_name});
COMMIT;
'''.format(db2_parcel_edits_analysis_table_name=db2_parcel_edits_analysis_table_name, db2_parcel_edits_analysis_view_name=db2_parcel_edits_analysis_view_name)

update_eee_parcel_edits_analysis_stmt = '''
BEGIN;
truncate table {table_name};
COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true);
COMMIT;
'''


def export_table_from_postgres(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    table_name=kwargs['table_name']
    stmt=stmt if stmt else '''select * from {table_name}'''.format(table_name=table_name)
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    #pg_hook.bulk_dump(table_name, csv_path)
    pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)


def load_table_to_postgres(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    table_name=kwargs['table_name']
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.bulk_load(table_name, csv_path)


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
    task_id='make_parcel_editing_staging1',
    dag=pipeline,
)

#----------------------------------------------------
# Read parcel editing layer from editing database:

extract_parcel_editing_records = PythonOperator(
    task_id='extract_parcel_editing_records',
    dag=pipeline,
    python_callable=export_table_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'tripoli', 'table_name': eee_parcel_edits_table_name, 'stmt': read_parcel_edits_sql},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_parcel_editing_staging1") }}/dor_parcels.csv'},
)

#----------------------------------------------------
# Standardize address comps with Passyunk:

standardize_address_comps = PythonOperator(
    task_id='standardize_address_comps',
    dag=pipeline,
    python_callable=check_address_comps,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_parcel_editing_staging1") }}','infile_name': 'dor_parcels', 'outfile_suffix':'_std'},
)

# Write to databridge2:
write_standardized_parcel_editing_records = PythonOperator(
    task_id='write_parcel_editing_layer',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_parcel_edits_std_table_name, 'stmt': write_parcel_edits_std_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_parcel_editing_staging1") }}/dor_parcels_std.csv'},
)

# -----------------------------------------------------------------
# Update hashes

update_parcel_editing_hash = PythonOperator(
    task_id='update_parcel_editing_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'parcel_edits_std', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories

update_parcel_editing_history = PythonOperator(
    task_id='update_parcel_editing_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'parcel_edits_std', 'hash_field': 'etl_hash'},
)

#------------------------------------------------------------------
# Update analyses

update_dor_pwd_overlap_analysis = PythonOperator(
    task_id='update_dor_pwd_overlap_analysis',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_dor_pwd_overlap_analysis_stmt},
)

update_dor_parcel_edits_analysis = PythonOperator(
    task_id='update_dor_parcel_edits_analysis',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'databridge2', 'stmt':update_db2_parcel_edits_analysis_stmt},
)

# ---------------------------------------------------
## Read transformed data from Databridge2 view:

extract_parcel_edits_analysis_records = PythonOperator(
    task_id='extract_parcel_edits_analysis_records',
    dag=pipeline,
    python_callable=export_table_from_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'databridge2', 'table_name': db2_parcel_edits_analysis_table_name, 'stmt': read_parcel_edits_analysis_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_parcel_editing_staging1") }}/parcel_edits_analysis.csv'},
)

# ---------------------------------------------------
## Write transformed data back to editing database:

write_parcel_edits_analysis_records = PythonOperator(
    task_id='write_eee_parcel_edits_analysis',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'tripoli', 'table_name': eee_parcel_edits_analysis_table_name, 'stmt': update_eee_parcel_edits_analysis_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_parcel_editing_staging1") }}/parcel_edits_analysis.csv'},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_parcel_editing_staging1") }}',
)


extract_parcel_editing_records.set_upstream(make_staging)
standardize_address_comps.set_upstream(extract_parcel_editing_records)
write_standardized_parcel_editing_records.set_upstream(standardize_address_comps)
write_standardized_parcel_editing_records.set_downstream(update_parcel_editing_hash)
update_parcel_editing_hash.set_downstream(update_parcel_editing_history)
update_parcel_editing_history.set_downstream(update_dor_pwd_overlap_analysis)
update_dor_pwd_overlap_analysis.set_downstream(update_dor_parcel_edits_analysis)
update_dor_parcel_edits_analysis.set_downstream(extract_parcel_edits_analysis_records)
extract_parcel_edits_analysis_records.set_downstream(write_parcel_edits_analysis_records)
write_parcel_edits_analysis_records.set_downstream(cleanup)
