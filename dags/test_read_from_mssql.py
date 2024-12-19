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

pipeline = DAG('etl_test_read_mssql', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG
table_name='Water_Delinquent_Accounts'

def read_mssql(**kwargs):
    hook = BaseHook
    db_conn_id = kwargs['templates_dict']['db_conn_id']
    table_name = kwargs['templates_dict']['table_name']
    params = hook.get_connection(db_conn_id)
    db_driver = 'ODBC Driver 17 for SQL Server'
    conn = pyodbc.connect(
        '''DRIVER={db_driver};SERVER={host};DATABASE={schema};UID={login};PWD={password}'''.format(db_driver=db_driver, host=params.host, schema=params.schema, login=params.login, password=params.password)
        )
    print(params.login, params.password, params.host, params.schema)
    print(conn)
    cursor=conn.cursor()
    stmt = '''select * from {table_name}'''.format(table_name=table_name)
    print(stmt)
    cursor.execute(stmt)
    rows = cursor.fetchall()
    print(rows)


read_mssql_test = PythonOperator(
    task_id='read_mssql_test',
    dag=pipeline,
    python_callable=read_mssql,
    provide_context=True,
    templates_dict={'db_conn_id':'revenue_data_warehouse', 'table_name':table_name},
)


