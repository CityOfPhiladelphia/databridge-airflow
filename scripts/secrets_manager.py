import json
import base64
import sys

import boto3
from botocore.exceptions import ClientError
import click


def get_secret(name, region):

    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=name
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

def build_bash_cmd(env, value):
    bash_cmd = 'export {}="{}"'.format(env, value)
    return bash_cmd

@click.command()
@click.option('--name', '-s', help='The name of the secret in AWS Secrets Manager')
@click.option('--key', '-k', help='The key of the secret')
@click.option('--env', '-e', help='The environment variable to set')
@click.option('--region', '-r', default='us-east-1', help='The region your secret is stored in AWS')
def main(name, key, env, region):
    secret = get_secret(name, region)
    value = secret[key]
    bash_cmd = build_bash_cmd(env, value)
    sys.stdout.write(bash_cmd)

if __name__ == '__main__':
    main()
