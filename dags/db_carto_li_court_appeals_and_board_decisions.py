from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.models import Variable

# ============================================================
# Defaults - these arguments apply to all operators

li_court_appeals_schema = Variable.get('schemas') + 'court_appeals.json'
li_board_decisions_schema = Variable.get('schemas') + 'board_decisions.json'


default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_db_carto_li_court_appeals_and_board_decisions_v0', default_args=default_args, schedule_interval='0 8 * * *')  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from db2 police account:

extract_court_appeals = GeopetlReadOperator(
    task_id='read_li_court_appeals',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/court_appeals.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.court_appeals',
    db_table_where='',
    db_timestamp=False,
)


extract_board_decisions = GeopetlReadOperator(
    task_id='read_li_board_decisions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/board_decisions.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.board_decisions',
    db_table_where='',
    db_timestamp=False,
)

# ----------------------------------------------------
# Write extracted files to Carto

write_court_appeals = CartoUpdateOperator(
    task_id='write_li_court_appeals',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/court_appeals.csv',
    db_conn_id='carto_phl',
    db_table_name='court_appeals',
    db_schema_json=li_court_appeals_schema,
    db_select_users=['publicuser', 'tileuser']
)

write_board_decisions = CartoUpdateOperator(
    task_id='write_li_board_decisions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/board_decisions.csv',
    db_conn_id='carto_phl',
    db_table_name='board_decisions',
    db_schema_json=li_board_decisions_schema,
    db_select_users=['publicuser', 'tileuser']
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}'
)

extract_court_appeals.set_upstream(make_staging)
extract_board_decisions.set_upstream(make_staging)

extract_court_appeals.set_downstream(write_court_appeals)
extract_board_decisions.set_downstream(write_board_decisions)

write_court_appeals.set_downstream(cleanup)
write_board_decisions.set_downstream(cleanup)
