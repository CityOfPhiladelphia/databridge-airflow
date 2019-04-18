from datetime import datetime, timedelta

from airflow import DAG
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from airflow.hooks.base_hook import BaseHook

from slack_notify_plugin import SlackNotificationOperator


def databridge_carto_dag_factory(
    table_schema,
    table_name,
    schedule_interval=None,
    retries=0):


    db_conn = BaseHook.get_connection('databridge')
    carto_conn = BaseHook.get_connection('carto_phl')

    dag_id = 'databridge_carto_{}'.format(table_name)

    default_args = {
        'owner': 'airflow',
        'on_success_callback': SlackNotificationOperator.success,
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': retries
    }

    dag = DAG(dag_id,
        start_date=datetime.now() - timedelta(days=1),
        schedule_interval=schedule_interval,
        default_args=default_args,
        max_active_runs=1,
    )

    databridge_to_s3 = AWSBatchOperator(
        job_name='db_to_s3_{}_{}'.format(table_schema, table_name),
        job_definition='test_extract_and_load_to_databridge',
        job_queue='databridge-airflow',
        region_name='us-east-1',
        overrides={'command': ["python3 /extract_and_load_to_databridge.py", 
                           "extract", 
                           "db_type={}".format(db_conn.conn_type), 
                           "db_host={}".format(db_conn.host), 
                           "db_user={}".format(db_conn.login), 
                           "db_password={}".format(db_conn.password), 
                           "db_name={}".format(db_conn.extra), 
                           "db_port={}".format(db_conn.port), 
                           "db_table_schema={}".format(table_schema), 
                           "db_table_name={}".format(table_name), 
                           "s3_bucket=citygeo-airflow-databridge2",]},
        task_id='db_to_s3_{}_{}'.format(table_schema, table_name),
        dag=dag,
    )
    
    s3_to_carto = AWSBatchOperator(
        job_name='s3_to_carto_{}_{}'.format(table_schema, table_name),
        job_definition='test_extract_and_load_to_databridge',
        job_queue='databridge-airflow',
        region_name='us-east-1',
        overrides={'command': ["python3 /extract_and_load_to_databridge.py",
                           "carto_update_table",
                           "carto_connection_string={}".format(carto_conn.password),
                           "s3_bucket=citygeo-airflow-databridge2",
                           "json_schema_file=gis_{}__{}.json".format(table_schema, table_name),]},
        task_id='s3_to_carto_{}_{}'.format(table_schema, table_name),
        dag=dag,
    )

    databridge_to_s3 >> s3_to_carto

    globals()[dag_id] = dag # Airflow looks at the module global vars for DAG type variables

databridge_carto_dag_factory('lni', 'li_appeals')
