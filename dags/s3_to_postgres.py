""""
This module creates dags to extract data from knack and load it to carto.
"""
from typing import List
from datetime import datetime, timedelta
import os
import yaml

from airflow import DAG

from airflow.operators.s3_to_postgres_plugin import S3ToPostgresOperator


dag_id = '01_test_s3_to_postgres'
schedule_interval = None
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 30, 0, 0, 0) - timedelta(hours=8)
}

with DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        default_args=default_args,
        max_active_runs=1,
) as dag:

    s3_to_postgres = S3ToPostgresOperator(
        schema='ais_sources',
        table='ais_zoning_documents',
        s3_bucket='citygeo-airflow-databridge2',
        s3_key='staging/ais/ais_zoning_documents.csv',
        postgres_conn_id='databridge2',
        copy_options=('FORMAT csv', 'HEADER true')
    )

    s3_to_postgres
