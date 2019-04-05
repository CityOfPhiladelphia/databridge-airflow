'''This script fetches all your secrets from AWS Secrets Manager and sets them 
as environment variables.'''

from collections import namedtuple
import base64
import os

import boto3
from botocore.exceptions import ClientError


class AirflowSecret():
    def __init__(self, secret_name, airflow_env):
        self.secret_name = secret_name
        self.airflow_env = airflow_env
        self.secret = self.get_secret(secret_name)

    @property
    def connection_uri(self):
        if self.secret.engine == 'oracle':
            import cx_Oracle

            dbname   = secret['dbname']
            username = secret['username']
            password = secret['password']
            host     = secret['host']
            port     = secret['port'] or 1521

            if host.endswith('__tns'):
                    host = host.replace('__tns', '')
                    conn_str = '{username}/{password}@{host}'.format(username, password, host)
            else:
                dsn = cx_Oracle.makedsn(host, port, database)
                conn_str = '{username}/{password}@{dsn}'.format(username, password, dsn)
            return conn_str
        elif self.secret.engine == 'postgres':
            import psycopg2

            dbname   = secret['db_name'],
            username = secret['username'],
            password = secret['password'],
            host     = secret['host']
            port     = secret['port'] or 5432

            conn_str = 'postgres://{username}:{password}@{host}:{port}/{dbname}'.format(username, password, host, port, dbname)
            return conn_str
        elif connection_type == 'slack':
            pass

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
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])

        return secret or decoded_binary_secret

SecretMetadata = namedtuple('Secret', ['secret_name', 'airflow_env'])

# Change theses to the names of your secrets in AWS Secrets Manager
# Mind the specific formatting of environment variables that Airflow expects
SECRET_METADATAS = (
    # SecretMetadata('databridge', 'AIRFLOW_CONN_DATABRIDGE_DEV'),
    AirflowSecret('databridge', 'AIRFLOW_CONN_DATABRIDGE_DEV')
)

for metadata in SECRET_METADATAS:
    # secret = get_secret(metadata.secret_name)
    # print(secret)
    print(metadata.connection_uri)
    #os.environ[metadata.airflow_env] = secret