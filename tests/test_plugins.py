import os
import sys
import pytest
from datetime import datetime

from airflow.models import TaskInstance
from airflow import DAG, settings

os.environ['ENVIRONMENT'] = 'TEST'
sys.path.append('../')
from airflow.operators.carto_plugin import S3ToCartoOperator
from airflow.operators.databridge_plugin import DataBridgeToS3Operator, S3ToDataBridge2Operator
from airflow.operators.knack_plugin import KnackToS3Operator
from airflow.operators.slack_notify_plugin import SlackNotificationOperator


@pytest.fixture(scope='module')
def global_data():
    return {
        'table_schema': 'schema',
        'table_name': 'table',
        'select_users': 'user',
        'object_id': '1'
    }

def test_s3_to_carto_operator(global_data):
    carto_operator = S3ToCartoOperator(
        table_schema=global_data['table_schema'],
        table_name=global_data['table_name'],
        select_users=global_data['select_users']
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

def test_databridge_to_s3_operator(global_data):
    databridge_to_s3 = DataBridgeToS3Operator(
        table_schema=global_data['table_schema'],
        table_name=global_data['table_name'],
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

def test_s3_to_databridge2_operator_non_numerical(global_data):
    s3_to_databridge2 = S3ToDataBridge2Operator(
        table_schema=global_data['table_schema'],
        table_name=global_data['table_name']
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
    
def test_s3_to_databridge2_operator_numerical(global_data):
    table_schema = '311'
    table_name = global_data['table_name']

    s3_to_databridge2 = S3ToDataBridge2Operator(
        table_schema=table_schema,
        table_name=table_name
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

def test_knack_to_s3_operator(global_data):
    knack_to_s3 = KnackToS3Operator(
        table_schema=global_data['table_schema'],
        table_name=global_data['table_name'],
        object_id=global_data['object_id']
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