import os
import sys

os.environ['ENVIRONMENT'] = 'TEST'
sys.path.append('../')
from airflow.operators.carto_plugin import S3ToCartoBatchOperator
from airflow.operators.oracle_to_s3_batch_plugin import OracleToS3BatchOperator
from airflow.operators.s3_to_postgres_batch_plugin import S3ToPostgresBatchOperator
from airflow.operators.knack_plugin import KnackToS3BatchOperator
from airflow.operators.airtable_plugin import AirtableToS3BatchOperator
from airflow.operators.batch_geocoder_plugin import BatchGeocoderOperator
from airflow.operators.slack_notify_plugin import SlackNotificationOperator


AIRTABLE_CONN_ID = 'airtable'
CARTO_CONN_ID = 'carto_phl'
DATABRIDGE_CONN_ID = 'databridge'
DATABRIDGE2_CONN_ID = 'databridge2'
KNACK_CONN_ID = 'knack'
TABLE_SCHEMA = 'schema'
TABLE_NAME = 'table'
NUMERICAL_TABLE_NAME = '311'
SELECT_USERS = 'user'
OBJECT_ID = '1'
INPUT_FILE = 'input.csv'
OUTPUT_FILE = 'output.csv'
QUERY_FIELDS = 'query_field'
AIS_FIELDS = 'lat,lon'

def test_s3_to_carto_batch_operator():
    carto_operator = S3ToCartoBatchOperator(
        conn_id=CARTO_CONN_ID,
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

def test_databridge_to_s3_batch_operator():
    databridge_to_s3 = OracleToS3BatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        conn_id=DATABRIDGE_CONN_ID,
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
    s3_to_databridge2 = S3ToPostgresBatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        conn_id=DATABRIDGE2_CONN_ID,
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
    
def test_s3_to_databridge2_batch_operator_numerical():
    s3_to_databridge2 = S3ToPostgresBatchOperator(
        table_schema=NUMERICAL_TABLE_NAME,
        table_name=TABLE_NAME,
        conn_id=DATABRIDGE2_CONN_ID,
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

def test_knack_to_s3_batch_operator():
    knack_to_s3 = KnackToS3BatchOperator(
        conn_id=KNACK_CONN_ID,
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        object_id=OBJECT_ID,
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

def test_airtable_to_s3_operator():
    airtable_to_s3 = AirtableToS3BatchOperator(
        table_schema=TABLE_SCHEMA,
        table_name=TABLE_NAME,
        conn_id=AIRTABLE_CONN_ID,
    )

    expected_command = [
        'extract-airtable',
        'extract-records',
        'login',
        'password',
        'table',
        '--s3-bucket=citygeo-airflow-databridge2',
        '--s3-key=staging/schema/table.csv'
    ]

    assert airtable_to_s3._command == expected_command

def test_batch_geocoder_operator():
    batch_geocoder = BatchGeocoderOperator(
        name='name',
        input_file=INPUT_FILE,
        output_file=OUTPUT_FILE,
        query_fields=QUERY_FIELDS,
        ais_fields=AIS_FIELDS
    )

    expected_command = [
        'batch_geocoder',
        'ais',
        '--input-file=input.csv',
        '--output-file=output.csv',
        '--ais-url=hostname',
        '--ais-key=password',
        '--ais-user=login',
        '--query-fields=query_field',
        '--ais-fields=lat,lon'
    ]

    assert batch_geocoder._command == expected_command
