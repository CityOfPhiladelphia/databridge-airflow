import pyodbc
import csv
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

pipeline = DAG('etl_parcel_editing_v1', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG
dest_table_name='gis_revenue_p.water_delinquent_accounts'

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


# ------------------------------------------------------------
# Make staging area

#make_staging = CreateStagingFolder(
#    task_id='make_staging',
#    dag=pipeline,
#)
#
##----------------------------------------------------
## Read parcel editing layer from editing database:
#
#extract_parcel_editing_records = MsSQLReadOperator(
#    task_id='read_water_delinquency_layer',
#    csv_path='{{ ti.xcom_pull("make__staging") }}/water_delinquent_accounts.csv',
#    dag=pipeline,
#    db_conn_id='revenue_data_warehouse',
#    db_table_name='dor_parcel_edits_addr_analysis',
#    db_fields='''objectid, recsub, basereg, mapreg, parcel, recmap, stcod, house, suf, frac, stex, stdir, stnam, stdessuf, unit, unit_type, 
#elev_flag, topelev, botelev, condoflag, matchflag, inactdate, orig_date, status, geoid, stdes, addr_source, addr_std,
#created_user, created_date, last_edited_user, last_edited_date, last_edit_reason, axiomaticid, comments, citygeocomments, Shape.ToString() as shape, citygeoedit, CURRENT_TIMESTAMP as etl_read_timestamp
#''',
#)
#
## Write to databridge2:
#write_standardized_parcel_editing_records = GeopetlWriteOperator(
#    task_id='write_parcel_editing_layer',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_parcel_editing_staging1") }}/dor_parcels_std.csv',
#    db_conn_id='databridge2',
#    db_table_name='dor.dor_parcel_edits_std1',
#)
#
# -----------------------------------------------------------------
# Cleanup - delete staging folder
#
#cleanup = DestroyStagingFolder(
#    task_id='cleanup_staging',
#    dag=pipeline,
#    dir='{{ ti.xcom_pull("make_staging") }}',
#)

