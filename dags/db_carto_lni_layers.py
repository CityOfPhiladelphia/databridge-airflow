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

appeals_schema = Variable.get('schemas') + 'db_lni_appeals.json'
unsafe_schema = Variable.get('schemas') + 'db_lni_unsafe.json'
imm_dang_schema = Variable.get('schemas') + 'db_lni_imm_dang.json'
board_decisions_schema = Variable.get('schemas') + 'db_lni_board_decisions.json'
case_investigations_schema = Variable.get('schemas') + 'db_lni_case_investigations.json'


default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_db_carto_lni_layers_v0', schedule_interval='0 11 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_db_carto_lni_layers_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from gis_lni in Databridge:

extract_appeals = GeopetlReadOperator(
    task_id='read_lni_appeals',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/appeals.csv',
    db_conn_id='databridge',
    db_table_name='gis_lni.appeals',
    db_table_where='',
    db_timestamp=False,
)

# extract_unsafe = GeopetlReadOperator(
#     task_id='read_lni_unsafe',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/unsafe.csv',
#     db_conn_id='databridge',
#     db_table_name='gis_lni.unsafe',
#     db_table_where='',
#     db_timestamp=False,
# )

# extract_imm_dang = GeopetlReadOperator(
#     task_id='read_lni_imm_dang',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/imm_dang.csv',
#     db_conn_id='databridge',
#     db_table_name='gis_lni.imm_dang',
#     db_table_where='',
#     db_timestamp=False,
# )

# extract_board_decisions = GeopetlReadOperator(
#     task_id='read_lni_board_decisions',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/board_decisions.csv',
#     db_conn_id='databridge',
#     db_table_name='gis_lni.board_decisions',
#     db_table_where='',
#     db_timestamp=False,
# )

# extract_case_investigations = GeopetlReadOperator(
#     task_id='read_lni_case_investigations',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/case_investigations.csv',
#     db_conn_id='databridge',
#     db_table_name='gis_lni.case_investigations',
#     db_table_where='',
#     db_timestamp=False,
# )

# ----------------------------------------------------
# Write extracted files to Carto

write_carto_appeals = CartoUpdateOperator(
    task_id='write_carto_appeals',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/appeals.csv',
    db_conn_id='carto_phl',
    db_table_name='appeals',
    db_schema_json=appeals_schema,
    db_select_users=['publicuser', 'tileuser']
)

# write_carto_unsafe = CartoUpdateOperator(
#     task_id='write_carto_unsafe',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/unsafe.csv',
#     db_conn_id='carto_phl',
#     db_table_name='unsafe',
#     db_schema_json=unsafe_schema,
#     db_select_users=['publicuser', 'tileuser']
# )

# write_carto_imm_dang = CartoUpdateOperator(
#     task_id='write_carto_imm_dang',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/imm_dang.csv',
#     db_conn_id='carto_phl',
#     db_table_name='imm_dang',
#     db_schema_json=imm_dang_schema,
#     db_select_users=['publicuser', 'tileuser']
# )

# write_carto_board_decisions = CartoUpdateOperator(
#     task_id='write_carto_board_decisions',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/board_decisions.csv',
#     db_conn_id='carto_phl',
#     db_table_name='board_decisions',
#     db_schema_json=board_decisions_schema,
#     db_select_users=['publicuser', 'tileuser']
# )

# write_carto_case_investigations = CartoUpdateOperator(
#     task_id='write_carto_case_investigations',
#     dag=pipeline,
#     csv_path='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}/case_investigations.csv',
#     db_conn_id='carto_phl',
#     db_table_name='case_investigations',
#     db_schema_json=case_investigations_schema,
#     db_select_users=['publicuser', 'tileuser']
# )

#------------------------------------------------------------------
# Refresh temp vw_appeals_est

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_db_carto_lni_layers_staging") }}')

extract_appeals.set_upstream(make_staging)
extract_appeals.set_downstream(write_carto_appeals)
write_carto_appeals.set_downstream(cleanup)

# extract_unsafe.set_upstream(make_staging)
# extract_unsafe.set_downstream(write_carto_unsafe)
# write_carto_unsafe.set_downstream(cleanup)

# extract_imm_dang.set_upstream(make_staging)
# extract_imm_dang.set_downstream(write_carto_imm_dang)
# write_carto_imm_dang.set_downstream(cleanup)

# extract_board_decisions.set_upstream(make_staging)
# extract_board_decisions.set_downstream(write_carto_board_decisions)
# write_carto_board_decisions.set_downstream(cleanup)

# extract_case_investigations.set_upstream(make_staging)
# extract_case_investigations.set_downstream(write_carto_case_investigations)
# write_carto_case_investigations.set_downstream(cleanup)
