'''This script fetches all your secrets from AWS Secrets Manager and writes them to stdout to be set as environment variables.'''

from collections import namedtuple
import json
import base64
import os
import sys

import boto3
from botocore.exceptions import ClientError


class AirflowSecret():
    def __init__(self, secret_name, airflow_env):
        self.secret_name = secret_name
        self.airflow_env = airflow_env
        self.secret = self.get_secret(secret_name)

    @property
    def connection_uri(self):
        secret = self.secret
        # Databases
        if 'engine' in secret:
            if secret['engine'] == 'oracle':
                import cx_Oracle

                dbname   = secret['dbname']
                username = secret['username']
                password = secret['password']
                host     = secret['host']
                port     = secret['port'] or 1521

                if host.endswith('__tns'):
                    host = host.replace('__tns', '')
                    conn_str = 'oracle+cx_oracle://{}/{}@{}'.format(username, password, host)
                else:
                    dsn = cx_Oracle.makedsn(host, port, dbname)
                    conn_str = 'oracle+cx_oracle://{}/{}@{}'.format(username, password, dsn)
            elif secret['engine'] == 'postgres':
                import psycopg2

                dbname   = secret['dbname']
                username = secret['username']
                password = secret['password']
                host     = secret['host']
                port     = secret['port'] or 5432

                conn_str = 'postgresql://{}:{}@{}:{}/{}'.format(username, password, host, port, dbname)
        elif 'connection_string' in secret:
            conn_str = secret['connection_string']
        elif 'fernet_key' in secret:
            conn_str = secret['fernet_key']
        return conn_str

    @staticmethod
    def get_secret(secret_name):

        REGION_NAME = 'us-east-1'

        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=REGION_NAME
        )

        try:
            get_secret_value_response = client.get_secret_value(
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                raise e
        else:
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        secret = json.loads(secret)
        return secret 

SECRETS = (
    AirflowSecret('databridge', 'AIRFLOW_CONN_DATABRIDGE'),
    AirflowSecret('databridge-dev', 'AIRFLOW_CONN_DATABRIDGE'),
    AirflowSecret('brt-viewer', 'AIRFLOW_CONN_BRT_VIEWER'),
    AirflowSecret('carto-prod', 'AIRFLOW_CONN_CARTO_PROD'),
    AirflowSecret('airflow-fernet', 'AIRFLOW__CORE__FERNET_KEY'),
    AirflowSecret('airflow-slack-dev', 'AIRFLOW_CONN_SLACK'),
)

for s in SECRETS:
    sys.stdout.write(s.airflow_env + '=' + '"' + s.connection_uri + '"' + '\n')
