import pyodbc
import csv
from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.doroem_parcel_report import check_address_comps
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

pipeline = DAG('etl_parcel_editing_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG
dest_table_name='DOR_PARCEL_EDITS_ADDR_ANALYSIS'


def truncate_mssql(**kwargs):
    hook = BaseHook
    db_conn_id = kwargs['templates_dict']['db_conn_id']
    dest_table_name = kwargs['templates_dict']['dest_table_name']
    params = hook.get_connection(db_conn_id)
    db_driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(
        '''DRIVER={db_driver};SERVER={host};DATABASE={schema};UID={login};PWD={password}'''.format(db_driver=db_driver, host=params.host, schema=params.schema, login=params.login, password=params.password)
        )
    cursor=conn.cursor()
    stmt = '''Truncate table {}'''.format(dest_table_name)
    cursor.execute(stmt)
    cursor.commit()


def write_mssql(**kwargs):
    hook = BaseHook
    test_file = kwargs['templates_dict']['test_file']
    db_conn_id = kwargs['templates_dict']['db_conn_id']
    dest_table_name = kwargs['templates_dict']['dest_table_name']
    params = hook.get_connection(db_conn_id)
    db_driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(
        '''DRIVER={db_driver};SERVER={host};DATABASE={schema};UID={login};PWD={password}'''.format(db_driver=db_driver, host=params.host, schema=params.schema, login=params.login, password=params.password)
        )
    cursor=conn.cursor()
    geom_field = 'shape'
    numeric_fields = ['topelev','botelev']
    srid=2272
    header=''
    with open(test_file, 'r') as f:
        header=f.readline().replace('\n', '')
    header_list = header.split(',')
    geom_position = header_list.index(geom_field)
    geom_placeholder = '''geometry::STGeomFromText( ?, {srid})'''.format(srid=srid)
    value_placeholders=['?'] * len(header_list)
    value_placeholders[geom_position] = geom_placeholder
    stmt = '''insert into {table_name}({header}) values({value_placeholders})'''.format(table_name=dest_table_name, header=header, value_placeholders=','.join(value_placeholders))
    with open(test_file, 'r') as f:
        reader = csv.DictReader(f)
        i=0
        for row in reader:
            i+=1
            if i % 1000 == 0:
                print(i)
            for numeric_field in numeric_fields:
                val = row.get(numeric_field)
                row[numeric_field] = float(val) if val.strip() else None
            prep_row = tuple(row.values())
            cursor.execute(stmt, prep_row)
        cursor.commit()

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_parcel_editing_staging',
    dag=pipeline,
)

#----------------------------------------------------
# Read parcel editing layer from editing database:

extract_parcel_editing_records = MsSQLReadOperator(
    task_id='read_parcel_editing_layer',
    csv_path='{{ ti.xcom_pull("make_parcel_editing_staging") }}/dor_parcels.csv',
    dag=pipeline,
    db_conn_id='citygeo_editing',
    db_table_name='dor_parcel_edits_addr_analysis',
    db_fields='''objectid, recsub, basereg, mapreg, parcel, recmap, stcod, house, suf, stex, stdir, stnam, stdessuf, unit, unit_type, 
elev_flag, topelev, botelev, condoflag, matchflag, inactdate, orig_date, status, geoid, stdes, addr_source, addr_std,
created_user, created_date, last_edited_user, last_edited_date, last_edit_reason, axiomaticid, comments, citygeocomments, Shape.ToString() as shape, citygeoedit, CURRENT_TIMESTAMP as etl_read_timestamp
''',
)

#----------------------------------------------------
# Standardize address comps with Passyunk:

standardize_address_comps = PythonOperator(
    task_id='standardize_address_comps',
    dag=pipeline,
    python_callable=check_address_comps,
    provide_context=True,
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_parcel_editing_staging") }}','infile_name': 'dor_parcels', 'outfile_suffix':'_std'},
)

# Write to databridge2:
write_standardized_parcel_editing_records = GeopetlWriteOperator(
    task_id='write_parcel_editing_layer',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_parcel_editing_staging") }}/dor_parcels_std.csv',
    db_conn_id='databridge2',
    db_table_name='dor.dor_parcel_edits_std',
)

# -----------------------------------------------------------------
# Update hashes

update_parcel_editing_hash = PythonOperator(
    task_id='update_parcel_editing_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'dor_parcel_edits_std', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories

update_parcel_editing_history = PythonOperator(
    task_id='update_parcel_editing_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'dor', 'table_name': 'dor_parcel_edits_std', 'hash_field': 'etl_hash'},
)

# ---------------------------------------------------
# Read transformed data from Databridge2 view:

read_transformed_dor_parcel_editing_data = GeopetlReadOperator(
    task_id='read_transformed_dor_parcel_editing_data',
    csv_path='{{ ti.xcom_pull("make_parcel_editing_staging") }}/vw_dor_parcel_edits_error_analysis.csv',
    dag=pipeline,
    db_conn_id='databridge2',
    db_table_name='dor.vw_dor_parcel_edits_error_analysis',
)

# ---------------------------------------------------
# Write transformed data back to editing database:

# First truncate table:
truncate_transformed_dor_parcel_editing_table = PythonOperator(
    task_id='truncate_transformed_dor_parcel_editing_table',
    dag=pipeline,
    python_callable=truncate_mssql,
    provide_context=True,
    templates_dict={'db_conn_id':'citygeo_editing', 'dest_table_name':dest_table_name},
)

write_transformed_dor_parcel_editing_data = PythonOperator(
    task_id='write_transformed_dor_parcel_editing_data',
    dag=pipeline,
    python_callable=write_mssql,
    provide_context=True,
    templates_dict={'db_conn_id':'citygeo_editing', 'test_file':'{{ ti.xcom_pull("make_parcel_editing_staging") }}/vw_dor_parcel_edits_error_analysis.csv', 'dest_table_name':dest_table_name},
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_parcel_editing_staging") }}',
)

extract_parcel_editing_records.set_upstream(make_staging)
standardize_address_comps.set_upstream(extract_parcel_editing_records)
write_standardized_parcel_editing_records.set_upstream(standardize_address_comps)
write_standardized_parcel_editing_records.set_downstream(update_parcel_editing_hash)
update_parcel_editing_hash.set_downstream(update_parcel_editing_history)
read_transformed_dor_parcel_editing_data.set_upstream(write_standardized_parcel_editing_records)
read_transformed_dor_parcel_editing_data.set_downstream(truncate_transformed_dor_parcel_editing_table)
write_transformed_dor_parcel_editing_data.set_upstream(truncate_transformed_dor_parcel_editing_table)
write_transformed_dor_parcel_editing_data.set_downstream(cleanup)
