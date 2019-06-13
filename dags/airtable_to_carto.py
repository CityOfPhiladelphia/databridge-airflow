""""
This module defines a dag to extract Immigration Services data from Airtable and load it to S3, then Carto.
"""
from datetime import datetime, timedelta
import os

from airflow import DAG

from airflow.operators.slack_notify_plugin import SlackNotificationOperator
from airflow.operators.airtable_plugin import AirtableToS3BatchOperator
from airflow.operators.batch_geocoder_plugin import BatchGeocoderOperator
from airflow.operators.carto_plugin import S3ToCartoBatchOperator


AIRTABLE_CONN_ID = TABLE_SCHEMA = 'airtable'
TABLE_NAME = 'immigrant_services'
QUERY_FIELDS = 'street_address'
AIS_FIELDS = 'lat,lon,shape'
CARTO_PHL_CONN_ID = 'carto_phl'
CARTO_TABLE_NAME = 'oia_services'
SELECT_USERS = 'publicuser,tileuser'

DAG_ID = 'airtable__immigrant_services'

default_args = {
        'owner': 'airflow',
        'start_date': datetime(2019, 5, 30, 0, 0, 0) - timedelta(hours=8),
        'on_failure_callback': SlackNotificationOperator.failed,
        'retries': 2 if os.environ['ENVIRONMENT'] == 'prod' else 0,
        'retry_delay': timedelta(minutes=5),
    }

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        max_active_runs=1,
        schedule_interval=None,
) as dag:

    airtable_to_s3 = AirtableToS3BatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        conn_id=AIRTABLE_CONN_ID)

    input_file = 's3://{}/{}'.format(
        airtable_to_s3.S3_BUCKET,
        airtable_to_s3.csv_s3_key)

    output_file = 's3://{}/{}.csv'.format(
        airtable_to_s3.S3_BUCKET,
        CARTO_TABLE_NAME)

    batch_geocoder = BatchGeocoderOperator(
        name='immigrant_services',
        input_file=input_file,
        output_file=output_file,
        query_fields=QUERY_FIELDS,
        ais_fields=AIS_FIELDS)

    s3_to_carto = S3ToCartoBatchOperator(
        conn_id=CARTO_PHL_CONN_ID,
        table_schema=TABLE_SCHEMA,
        table_name=CARTO_TABLE_NAME,
        select_users=SELECT_USERS,
        pool='carto')

    airtable_to_s3 >> batch_geocoder >> s3_to_carto
