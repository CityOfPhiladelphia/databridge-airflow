from airflow import DAG
import pyodbc
import csv
from airflow.hooks.base_hook import BaseHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import MsSQLReadOperator
from airflow.utils.slack import slack_failed_alert, slack_success_alert
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

pipeline = DAG('etl_test_write_mssql', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG
test_file = '/tmp/tmp3jvbbgxa/vw_dor_parcel_edits_error_analysis.csv'
dest_table_name='DOR_PARCEL_EDITS_ADDR_ANALYSIS'

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
    print(stmt)
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
 
#            geom=row.get(geom_field)
#            prep_geom='''SRID={srid};{geom}'''.format(geom=geom, srid=srid)
#            row[geom_field] = prep_geom
            prep_row = tuple(row.values())
#            print(prep_row)
#            prep_stmt = stmt.format(table_name=dest_table_name, header=header, values=prep_row)
#            print(i, prep_stmt)
#            if i < 10:
#                print(prep_stmt)
#            else:
#                raise
            cursor.execute(stmt, prep_row)
    cursor.commit()


write_mssql_test = PythonOperator(
    task_id='write_mssql_test',
    dag=pipeline,
    python_callable=write_mssql,
    provide_context=True,
    templates_dict={'db_conn_id':'citygeo_editing', 'test_file':test_file, 'dest_table_name':dest_table_name},
)


