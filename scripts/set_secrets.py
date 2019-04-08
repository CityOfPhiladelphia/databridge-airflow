'''This script fetches all your secrets from AWS Secrets Manager and writes them to stdout to be set as environment variables.'''

from collections import namedtuple
import json
import base64
import os
import sys

import boto3
from botocore.exceptions import ClientError


class AirflowSecret():
    def __init__(self, secret_name):
        self.secret_name = secret_name
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

                bash_cmd = '''airflow connections \
                                      -add \
                                      --conn_id={} \
                                      --conn_type=Oracle \
                                      --conn_host={} \
                                      --conn_login={} \
                                      --conn_password={} \
                                      --conn_port={}''' \
                                      .format(dbname, host, username, password, port)             
            elif secret['engine'] == 'postgres':
                import psycopg2

                dbname   = secret['dbname']
                username = secret['username']
                password = secret['password']
                host     = secret['host']
                port     = secret['port'] or 5432

                bash_cmd = '''airflow connections 
                                      -add \
                                      --conn_id={} \
                                      --conn_type=Postgres \
                                      --conn_host={} \
                                      --conn_login={} \
                                      --conn_password={} \
                                      --conn_port={}''' \
                                      .format(dbname, host, username, password, port)  
        elif 'airflow-slack-dev' in secret:
            login = secret["airflow-slack-dev"].split(':')[0]
            password = secret["airflow-slack-dev"].split(':')[1]
            bash_cmd = '''airflow connections 
                                  -add \
                                  --conn_id=slack \
                                  --conn_type=Postgres \
                                  --conn_host=https://hooks.slack.com/services \
                                  --conn_login={} \
                                  --conn_password={}''' \
                                  .format(login, password)
        elif 'airflow-slack-test' in secret:
            login = secret["airflow-slack-test"].split(':')[0]
            password = secret["airflow-slack-test"].split(':')[1]
            bash_cmd = '''airflow connections 
                                  -add \
                                  --conn_id=slack \
                                  --conn_type=Postgres \
                                  --conn_host=https://hooks.slack.com/services \
                                  --conn_login={} \
                                  --conn_password={}''' \
                                  .format(login, password)
        elif 'fernet_key' in secret:
            bash_cmd = f'export AIRFLOW__CORE__FERNET_KEY={secret["fernet_key"]}'
        return bash_cmd

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
    AirflowSecret('databridge'),
    AirflowSecret('databridge-dev'),
    AirflowSecret('brt-viewer'),
    AirflowSecret('carto-prod'),
    AirflowSecret('airflow-fernet'),
    AirflowSecret('airflow-slack-dev'),
)

for s in SECRETS:
    sys.stdout.write(s.airflow_env + '=' + '"' + s.connection_uri + '"' + '\n')
