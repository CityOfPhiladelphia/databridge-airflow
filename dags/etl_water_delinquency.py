from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators import MsSQLReadOperator
from airflow.operators import GeopetlReadOperator
from airflow.operators import GeopetlWriteOperator
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from datetime import datetime, timedelta
from airflow.models import Variable


beverage_tax_registration_data_schema = Variable.get('schemas') + 'beverage_tax_registration_data.json'

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

pipeline = DAG('etl_water_delinquency_to_db_v0', schedule_interval=None, default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_water_delinquency = MsSQLReadOperator(
    task_id='read_water_delinquent_accounts',
    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/water_delinquent_accounts.csv',
    dag=pipeline,
    db_conn_id='revenue_data_warehouse',
    db_table_name='Water_Delinquent_Accounts',
    db_fields='', 
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_water_delinquency = GeopetlWriteOperator(
    task_id='write_water_delinquency',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/water_delinquent_accounts.csv',
    db_conn_id='databridge',
    db_table_name='gis_revenue_p.water_delinquent_accounts',
)


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

extract_water_delinquency.set_upstream(make_staging)
extract_water_delinquency.set_downstream(write_water_delinquency)
write_water_delinquency.set_downstream(cleanup)
