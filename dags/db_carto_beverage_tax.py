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

pipeline = DAG('etl_bevtax_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)  # TODO: Look up how to schedule a DAG

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_bevtax_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from DOR

extract_beverage_tax_registration_data = MsSQLReadOperator(
    task_id='read_eGov3_TaxpayerRegistration_V',
    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/eGov3_TaxpayerRegistration_V.csv',
    dag=pipeline,
    db_conn_id='bevtax',
    db_table_name='eGov3_TaxpayerRegistration_V',
    db_fields='RegistrationType, OrganisationName, DBAName, BusinessAddress, BusinessUnitApt, BusinessCity, BusinessState, BusinessZipCode, BusinessPhone, BusinessPhExt, BusinessWebSite, StartDate, EndDate', 
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_beverage_tax_registration_data = GeopetlWriteOperator(
    task_id='write_eGov3_TaxpayerRegistration_V',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/eGov3_TaxpayerRegistration_V.csv',
    db_conn_id='databridge2',
    db_table_name='revenue.egov3_taxpayerregistration_v',
)

# ----------------------------------------------------
# Read transformed data from Databridge

read_transformed_beverage_tax_registration_data = GeopetlReadOperator(
    task_id='read_transformed_beverage_tax_registration_data',
    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/vw_beverage_tax_registration_data.csv',
    dag=pipeline,
    db_conn_id='databridge2',
    db_table_name='revenue.vw_beverage_tax_registration_data',
)

# ----------------------------------------------------
# Write transformed file to Carto
write_transformed_beverage_tax_registration_data = CartoUpdateOperator(
    task_id='write_transformed_beverage_tax_registration_data',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_bevtax_staging") }}/vw_beverage_tax_registration_data.csv',
    db_conn_id='carto_phl',
    db_table_name='beverage_tax_registration_data',
    db_schema_json=beverage_tax_registration_data_schema,
    db_select_users=['publicuser', 'tileuser']
)

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_bevtax_staging") }}',
)

extract_beverage_tax_registration_data.set_upstream(make_staging)
extract_beverage_tax_registration_data.set_downstream(write_beverage_tax_registration_data)
write_beverage_tax_registration_data.set_downstream(read_transformed_beverage_tax_registration_data)
read_transformed_beverage_tax_registration_data.set_downstream(write_transformed_beverage_tax_registration_data)
write_transformed_beverage_tax_registration_data.set_downstream(cleanup)

