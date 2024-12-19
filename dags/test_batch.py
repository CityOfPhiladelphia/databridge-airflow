from airflow import DAG
from airflow.contrib.operators.awsbatch_operator import AWSBatchOperator
from datetime import datetime, timedelta
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.models import Variable

# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2019, 2, 11, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
    'on_success_callback': slack_success_alert,
    'provide_context': True
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('test_batch', schedule_interval='0 5 * * *', default_args=default_args)

batch_test = AWSBatchOperator(
    job_name='airflow-batch-test',
    job_definition='test-3',
    job_queue='databridge-airflow4',
    region_name='us-east-1',
    overrides={'command': ["test_batch.sh", " 10"]},
    task_id='test-3',
    dag=pipeline
)

batch_test
