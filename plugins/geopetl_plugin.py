import sys
import logging
import csv
import petl as etl
import geopetl
import cx_Oracle
import psycopg2
from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


csv.field_size_limit(sys.maxsize)


class GeopetlHook(BaseHook):
    SCHEMAS = {
        'postgres': 'postgis',
        'oracle': 'oracle-stgeom',
    }

    def __init__(self, db_conn_id, conn=None):
        self.db_conn_id = db_conn_id
        self.conn = conn
        self.conn_str = None
        self.conn_type = None

    def get_conn_str(self):
        if self.conn_str is None:
            params = self.get_connection(self.db_conn_id)
            self.conn_type = params.conn_type
            self.conn_str = '{schema}://{auth}@{host}{port}{path}'.format(
#                schema=self.SCHEMAS[params.conn_type],
                schema = params.schema,
                auth=params.login + ':' + params.password,
                host=params.host,
                port=(':' + str(params.port) if params.port else ''),
                path=('/' + params.extra if params.extra else params.schema if params.schema else '')
            ) if params.schema else '{auth}@{host}{port}{path}'.format(
#                schema=self.SCHEMAS[params.conn_type],
                auth=params.login + ':' + params.password,
                host=params.host,
                port=(':' + str(params.port) if params.port else ''),
                path=('/' + params.extra if params.extra else params.schema if params.schema else '')
            )
        logging.info(self.conn_str)
        return self.conn_str

    def get_conn(self):
        if self.conn is None:
            params = self.get_connection(self.db_conn_id)
            self.conn_type = params.conn_type
            if self.conn_type == 'oracle' and not params.schema:
                self.dsn = cx_Oracle.makedsn(params.host, params.port, service_name=params.extra)
                self.conn = cx_Oracle.connect(user=params.login, password=params.password, dsn=self.dsn)
                return self.conn
            elif self.conn_type == 'postgres':
                self.conn = psycopg2.connect(dbname=params.schema, user=params.login, password=params.password, host=params.host, port=params.port)
                return self.conn
#            else:
#                conn_string = self.get_conn_str()
            if self.conn_type not in self.SCHEMAS:
                raise AirflowException('Could not create Geopetl connection for connection type {}'.format(self.conn_type))
            logging.info('Establishing connection to {}'.format(self.db_conn_id))
#            if self.conn_type == 'postgres':
#                self.conn = psycopg2.connect('postgresql://' + conn_string)
#            elif self.conn_type == 'oracle':
#                self.conn = cx_Oracle.connect(conn_string)
        return self.conn


class GeopetlReadOperator(BaseOperator):

    template_fields = ('db_table_name','csv_path',)
    ui_color = '#88ccff'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 csv_path,
                 db_table_name=None,
                 db_table_where=None,
                 db_field_overrides=None,
                 sql_override=None,
                 db_timestamp=True,
                 db_sql=None,
                 *args, **kwargs):
        super(GeopetlReadOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.csv_path = csv_path
        self.db_table_where = db_table_where
        self.db_field_overrides = db_field_overrides or {}
        self.sql_override = sql_override
        self.db_timestamp = db_timestamp
        self.db_sql = db_sql

    def execute(self, context):
        logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = GeopetlHook(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()
        if self.hook.conn_type == 'postgres':
            if self.db_sql:
                etl.frompostgis(self.conn, self.db_sql).tocsv(self.csv_path) 
            else:
                etl.frompostgis(self.conn, self.db_table_name).tocsv(self.csv_path)
        elif self.hook.conn_type == 'oracle':
            etl.fromoraclesde(self.conn, self.db_table_name, timestamp=self.db_timestamp).tocsv(self.csv_path)
        rows = etl.fromcsv(self.csv_path)
        #print(etl.look(rows))

        logging.info("Done!")


class GeopetlWriteOperator(BaseOperator):
    template_fields = ('db_table_name','csv_path',)
    ui_color = '#88ccff'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_table_name,
                 csv_path,
                 db_table_where=None,
                 db_field_overrides=None,
                 sql_override=None,
                 db_sql=None,
                 *args, **kwargs):
        super(GeopetlWriteOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.csv_path = csv_path
        self.db_table_where = db_table_where
        self.db_field_overrides = db_field_overrides or {}
        self.sql_override = sql_override

    def execute(self, context):
        logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = GeopetlHook(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()
        if self.hook.conn_type == 'postgres':
            rows = etl.fromcsv(self.csv_path)
#            try:
            rows.topostgis(self.conn, self.db_table_name)
#            except Exception as e:
#                print(2)
#                print(e)
#                etl.topostgis(rows, self.conn, self.db_table_name)
        elif self.hook.conn_type == 'oracle':
            etl.fromcsv(self.csv_path).tooraclesde(self.conn, self.db_table_name)

        logging.info("Done!")



class GeopetlPlugin(AirflowPlugin):
    name = "geopetl_plugin"
    operators = [GeopetlReadOperator, GeopetlWriteOperator]
    hooks = [GeopetlHook]
