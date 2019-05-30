import os
import sys
import pytest
import json
from datetime import datetime

from airflow.models import TaskInstance
from airflow import DAG, settings

os.environ['ENVIRONMENT'] = 'TEST'
sys.path.append('../')
from airflow.operators.carto_plugin import S3ToCartoBatchOperator, S3ToCartoLambdaOperator
from airflow.operators.databridge_plugin import (
    DataBridgeToS3BatchOperator,
    S3ToDataBridge2BatchOperator, S3ToDataBridge2LambdaOperator,
)
from airflow.operators.knack_plugin import KnackToS3BatchOperator, KnackToS3LambdaOperator
from airflow.operators.slack_notify_plugin import SlackNotificationOperator


TABLE_SCHEMA = 'schema'
TABLE_NAME = 'table'
NUMERICAL_TABLE_NAME = '311'
SELECT_USERS = 'user'
OBJECT_ID = '1'

def test_s3_to_carto_batch_operator():
    carto_operator = S3ToCartoBatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        select_users=SELECT_USERS
    )

    expected_command = [
        'databridge_etl_tools',
        'cartoupdate',
        '--table_name=table',
        '--connection_string=password',
        '--s3_bucket=citygeo-airflow-databridge2',
        '--json_schema_s3_key=schemas/schema/table.json',
        '--csv_s3_key=staging/schema/table.csv',
        '--select_users=user'
    ]

    assert carto_operator._command == expected_command

def test_s3_to_carto_lambda_operator():
    carto_operator = S3ToCartoLambdaOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        select_users=SELECT_USERS
    )

    expected_payload = json.dumps({
        'command_name': 'cartoupdate',
        'table_name': 'table',
        'connection_string': 'password',
        's3_bucket': 'citygeo-airflow-databridge2',
        'json_schema_s3_key': 'schemas/schema/table.json',
        'csv_s3_key': 'staging/schema/table.csv',
        'select_users': 'user'
    })

    assert carto_operator.payload == expected_payload

def test_databridge_to_s3_batch_operator():
    databridge_to_s3 = DataBridgeToS3BatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
    )

    expected_command = [
        'databridge_etl_tools',
        'extract',
        '--table_name=table',
        '--table_schema=gis_schema',
        '--connection_string=login/password@(DESCRIPTION=(ADDRESS=(PROTOCOL=TCP)(HOST=localhost)(PORT=1521))(CONNECT_DATA=(SID=db_name)))',
        '--s3_bucket=citygeo-airflow-databridge2',
        '--s3_key=staging/schema/table.csv',
    ]

    assert databridge_to_s3._command == expected_command

def test_s3_to_databridge2_batch_operator_non_numerical():
    s3_to_databridge2 = S3ToDataBridge2BatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME
    )

    expected_command = [
        'databridge_etl_tools',
        'load',
        '--table_name=table',
        '--table_schema=schema',
        '--connection_string=postgresql://login:password@localhost:5432/db_name',
        '--s3_bucket=citygeo-airflow-databridge2',
        '--json_schema_s3_key=schemas/schema/table.json',
        '--csv_s3_key=staging/schema/table.csv',
    ]

    assert s3_to_databridge2._command == expected_command

def test_s3_to_databridge2_lambda_operator_non_numerical():
    s3_to_databridge2 = S3ToDataBridge2LambdaOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME
    )

    expected_payload = json.dumps({
        'command_name': 'load',
        'table_name': 'table',
        'table_schema': 'schema',
        'connection_string': 'postgresql://login:password@localhost:5432/db_name',
        's3_bucket': 'citygeo-airflow-databridge2',
        'json_schema_s3_key': 'schemas/schema/table.json',
        'csv_s3_key': 'staging/schema/table.csv'
    })

    assert s3_to_databridge2.payload == expected_payload
    
def test_s3_to_databridge2_batch_operator_numerical():
    s3_to_databridge2 = S3ToDataBridge2BatchOperator(
        table_schema=NUMERICAL_TABLE_NAME,
        table_name=TABLE_NAME
    )

    expected_command = [
        'databridge_etl_tools',
        'load',
        '--table_name=table',
        '--table_schema=threeoneone',
        '--connection_string=postgresql://login:password@localhost:5432/db_name',
        '--s3_bucket=citygeo-airflow-databridge2',
        '--json_schema_s3_key=schemas/311/table.json',
        '--csv_s3_key=staging/311/table.csv',
    ]

    assert s3_to_databridge2._command == expected_command

def test_s3_to_databridge2_lambda_operator_numerical():
    s3_to_databridge2 = S3ToDataBridge2LambdaOperator(
        table_schema=NUMERICAL_TABLE_NAME,
        table_name=TABLE_NAME
    )

    expected_payload = json.dumps({
        'command_name': 'load',
        'table_name': 'table',
        'table_schema': 'threeoneone',
        'connection_string': 'postgresql://login:password@localhost:5432/db_name',
        's3_bucket': 'citygeo-airflow-databridge2',
        'json_schema_s3_key': 'schemas/311/table.json',
        'csv_s3_key': 'staging/311/table.csv'
    })

    assert s3_to_databridge2.payload == expected_payload

def test_knack_to_s3_batch_operator():
    knack_to_s3 = KnackToS3BatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        object_id=OBJECT_ID
    )

    expected_command = [
        'extract-knack',
        'extract-records',
        'knack_application_id',
        'knack_api_key',
        '1',
        '--s3_bucket=citygeo-airflow-databridge2',
        '--s3_key=staging/schema/table.csv',
    ]

    assert knack_to_s3._command == expected_command

def test_knack_to_s3_lambda_operator():
    knack_to_s3 = KnackToS3LambdaOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        object_id=OBJECT_ID
    )

    expected_payload = json.dumps({
        'command_name': 'extract-records',
        'app-id': 'knack_application_id',
        'app-key': 'knack_api_key',
        'object-id': '1',
        's3_bucket': 'citygeo-airflow-databridge2',
        's3_key': 'staging/schema/table.csv'
    })

    assert knack_to_s3.payload == expected_payload
