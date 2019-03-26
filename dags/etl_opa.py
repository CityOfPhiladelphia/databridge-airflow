from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from datetime import datetime, timedelta


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 1, 15, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_opa_geopetl_v0', schedule_interval='0 5 * * *', default_args=default_args)

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract - read files from OPA

extract_properties = GeopetlReadOperator(
    task_id='read_properties',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/properties.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.properties',
    db_table_where='',
)

extract_propertycharacteristics = GeopetlReadOperator(
    task_id='read_propertycharacteristics',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/propertycharacteristics.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.propertycharacteristics',
    db_table_where='',
)

extract_recordhistories = GeopetlReadOperator(
    task_id='read_recordhistories',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/recordhistories.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.recordhistories',
    db_table_where='',
)

extract_sales = GeopetlReadOperator(
    task_id='read_sales',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/sales.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.sales',
    db_table_where='',
)

extract_streetcodes = GeopetlReadOperator(
    task_id='read_streetcodes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/streetcodes.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.streetcodes',
    db_table_where='',
)

extract_owners = GeopetlReadOperator(
    task_id='read_owners',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owners.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.owners',
    db_table_where='',
)

extract_homestead = GeopetlReadOperator(
    task_id='read_homestead',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.homestead',
    db_table_where='',
)

extract_dimensions = GeopetlReadOperator(
    task_id='read_dimensions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dimensions.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.dimensions',
    db_table_where='',
)

extract_buildingcodes = GeopetlReadOperator(
    task_id='read_buildingcodes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/buildingcodes.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.buildingcodes',
    db_table_where='',
)

extract_categorycodes = GeopetlReadOperator(
    task_id='read_categorycodes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/categorycodes.csv',
    db_conn_id='brt-viewer',
    db_table_name='brt_admin.categorycodes',
    db_table_where='',
)

# ----------------------------------------------------
# Write extracted files to Databridge

write_properties = GeopetlWriteOperator(
    task_id='write_properties',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/properties.csv',
    db_conn_id='databridge2',
    db_table_name='opa.properties',
)

write_propertycharacteristics = GeopetlWriteOperator(
    task_id='write_propertycharacteristics',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/propertycharacteristics.csv',
    db_conn_id='databridge2',
    db_table_name='opa.propertycharacteristics',
)

write_recordhistories = GeopetlWriteOperator(
    task_id='write_recordhistories',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/recordhistories.csv',
    db_conn_id='databridge2',
    db_table_name='opa.recordhistories',
)

write_sales = GeopetlWriteOperator(
    task_id='write_sales',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/sales.csv',
    db_conn_id='databridge2',
    db_table_name='opa.sales',
)

write_streetcodes = GeopetlWriteOperator(
    task_id='write_streetcodes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/streetcodes.csv',
    db_conn_id='databridge2',
    db_table_name='opa.streetcodes',
)

write_buildingcodes = GeopetlWriteOperator(
    task_id='write_buildingcodes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/buildingcodes.csv',
    db_conn_id='databridge2',
    db_table_name='opa.buildingcodes',
)

write_categorycodes = GeopetlWriteOperator(
    task_id='write_categorycodes',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/categorycodes.csv',
    db_conn_id='databridge2',
    db_table_name='opa.categorycodes',
)

write_dimensions = GeopetlWriteOperator(
    task_id='write_dimensions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/dimensions.csv',
    db_conn_id='databridge2',
    db_table_name='opa.dimensions',
)

write_homestead = GeopetlWriteOperator(
    task_id='write_homestead',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/homestead.csv',
    db_conn_id='databridge2',
    db_table_name='opa.homestead',
)

write_owners = GeopetlWriteOperator(
    task_id='write_owners',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/owners.csv',
    db_conn_id='databridge2',
    db_table_name='opa.owners',
)

# -----------------------------------------------------------------
# Update hashes

update_properties_hash = PythonOperator(
    task_id='update_properties_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'properties', 'hash_field': 'etl_hash'},
)

update_propertycharacteristics_hash = PythonOperator(
    task_id='update_propertycharacteristics_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'propertycharacteristics', 'hash_field': 'etl_hash'},
)

update_recordhistories_hash = PythonOperator(
    task_id='update_recordhistories_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'recordhistories', 'hash_field': 'etl_hash'},
)

update_sales_hash = PythonOperator(
    task_id='update_sales_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_streetcodes_hash = PythonOperator(
    task_id='update_streetcodes_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'streetcodes', 'hash_field': 'etl_hash'},
)

update_owners_hash = PythonOperator(
    task_id='update_owners_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'owners', 'hash_field': 'etl_hash'},
)

update_homestead_hash = PythonOperator(
    task_id='update_homestead_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'homestead', 'hash_field': 'etl_hash'},
)

update_dimensions_hash = PythonOperator(
    task_id='update_dimensions_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'dimensions', 'hash_field': 'etl_hash'},
)

update_buildingcodes_hash = PythonOperator(
    task_id='update_buildingcodes_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'buildingcodes', 'hash_field': 'etl_hash'},
)

update_categorycodes_hash = PythonOperator(
    task_id='update_categorycodes_hash',
    dag=pipeline,
    python_callable=update_hash_fields,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'categorycodes', 'hash_field': 'etl_hash'},
)

# -----------------------------------------------------------------
# Update histories

update_properties_history = PythonOperator(
    task_id='update_properties_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'properties', 'hash_field': 'etl_hash'},
)

update_propertycharacteristics_history = PythonOperator(
    task_id='update_propertycharacteristics_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'propertycharacteristics', 'hash_field': 'etl_hash'},
)

update_recordhistories_history = PythonOperator(
    task_id='update_recordhistories_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'recordhistories', 'hash_field': 'etl_hash'},
)

update_sales_history = PythonOperator(
    task_id='update_sales_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'sales', 'hash_field': 'etl_hash'},
)

update_streetcodes_history = PythonOperator(
    task_id='update_streetcodes_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'streetcodes', 'hash_field': 'etl_hash'},
)

update_owners_history = PythonOperator(
    task_id='update_owners_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'owners', 'hash_field': 'etl_hash'},
)

update_homestead_history = PythonOperator(
    task_id='update_homestead_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'homestead', 'hash_field': 'etl_hash'},
)

update_dimensions_history = PythonOperator(
    task_id='update_dimensions_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'dimensions', 'hash_field': 'etl_hash'},
)

update_buildingcodes_history = PythonOperator(
    task_id='update_buildingcodes_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'buildingcodes', 'hash_field': 'etl_hash'},
)

update_categorycodes_history = PythonOperator(
    task_id='update_categorycodes_history',
    dag=pipeline,
    python_callable=update_history_table,
    op_kwargs={'db_conn_id':'databridge2', 'table_schema':'opa', 'table_name': 'categorycodes', 'hash_field': 'etl_hash'},
)





# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

#make_staging >> extract_properties >> write_to_databridge >> cleanup

extract_properties.set_upstream(make_staging)
extract_propertycharacteristics.set_upstream(make_staging)
extract_recordhistories.set_upstream(make_staging)
extract_sales.set_upstream(make_staging)
extract_streetcodes.set_upstream(make_staging)
extract_buildingcodes.set_upstream(make_staging)
extract_categorycodes.set_upstream(make_staging)
extract_dimensions.set_upstream(make_staging)
extract_homestead.set_upstream(make_staging)
extract_owners.set_upstream(make_staging)

extract_properties.set_downstream(write_properties)
extract_propertycharacteristics.set_downstream(write_propertycharacteristics)
extract_recordhistories.set_downstream(write_recordhistories)
extract_sales.set_downstream(write_sales)
extract_streetcodes.set_downstream(write_streetcodes)
extract_buildingcodes.set_downstream(write_buildingcodes)
extract_categorycodes.set_downstream(write_categorycodes)
extract_dimensions.set_downstream(write_dimensions)
extract_homestead.set_downstream(write_homestead)
extract_owners.set_downstream(write_owners)

write_properties.set_downstream(update_properties_hash)
write_propertycharacteristics.set_downstream(update_propertycharacteristics_hash)
write_recordhistories.set_downstream(update_recordhistories_hash)
write_sales.set_downstream(update_sales_hash)
write_streetcodes.set_downstream(update_streetcodes_hash)
write_buildingcodes.set_downstream(update_buildingcodes_hash)
write_categorycodes.set_downstream(update_categorycodes_hash)
write_dimensions.set_downstream(update_dimensions_hash)
write_homestead.set_downstream(update_homestead_hash)
write_owners.set_downstream(update_owners_hash)

update_properties_hash.set_downstream(update_properties_history)
update_propertycharacteristics_hash.set_downstream(update_propertycharacteristics_history)
update_recordhistories_hash.set_downstream(update_recordhistories_history)
update_sales_hash.set_downstream(update_sales_history)
update_streetcodes_hash.set_downstream(update_streetcodes_history)
update_buildingcodes_hash.set_downstream(update_buildingcodes_history)
update_categorycodes_hash.set_downstream(update_categorycodes_history)
update_dimensions_hash.set_downstream(update_dimensions_history)
update_homestead_hash.set_downstream(update_homestead_history)
update_owners_hash.set_downstream(update_owners_history)

update_properties_history.set_downstream(cleanup)
update_propertycharacteristics_history.set_downstream(cleanup)
update_recordhistories_history.set_downstream(cleanup)
update_sales_history.set_downstream(cleanup)
update_streetcodes_history.set_downstream(cleanup)
update_buildingcodes_history.set_downstream(cleanup)
update_categorycodes_history.set_downstream(cleanup)
update_dimensions_history.set_downstream(cleanup)
update_homestead_history.set_downstream(cleanup)
update_owners_history.set_downstream(cleanup)
