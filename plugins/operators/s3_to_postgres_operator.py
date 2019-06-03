from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import boto3


class S3ToPostgresOperator(BaseOperator):

    template_fields = ()
    template_ext = ()
    ui_color = '#ededed'

    _columns = None

    @apply_defaults
    def __init__(
            self,
            schema,
            table,
            s3_bucket,
            s3_key,
            s3_region='us-east-1',
            postgres_conn_id='postgres_default',
            copy_options=tuple(),
            *args, **kwargs):
        self.schema = schema
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_region = s3_region
        self.postgres_conn_id = postgres_conn_id
        self.copy_options = copy_options

        super().__init__(task_id=self._task_id, *args, **kwargs)

    @property
    def _task_id(self) -> str:
        return 's3_to_postgres_{}_{}'.format(self.schema, self.table)

    @property
    def columns(self):
        if self._columns is None:
            s3 = boto3.resource('s3')
            obj = s3.Object(self.s3_bucket, self.s3_key)
            stream = obj.get()['Body'].iter_lines()
            self._columns = next(stream).decode('utf-8')
        return self._columns

    def execute(self, context):
        self.hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)

        copy_query = """
            SELECT aws_s3.table_import_from_s3(
                '{schema}.{table}',
                '{columns}',
                '({copy_options})',
                '{s3_bucket}',
                '{s3_key}',
                '{s3_region}'
            );
        """.format(schema=self.schema,
                   table=self.table,
                   columns=self.columns,
                   copy_options=','.join(self.copy_options),
                   s3_bucket=self.s3_bucket,
                   s3_key=self.s3_key,
                   s3_region=self.s3_region
                   )

        self.log.info('Executing COPY command...')
        self.hook.run(copy_query)
        self.log.info("COPY command complete...")
