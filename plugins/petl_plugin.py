import os
import sys
import logging
import csv
import petl as etl
import cx_Oracle
import psycopg2
from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


csv.field_size_limit(sys.maxsize)

class PetlHook(BaseHook):
    SCHEMAS = {
        'postgres': 'postgis',
        'oracle': 'oracle-stgeom',
    }

    def __init__(self, db_conn_id, conn=None):
        self.db_conn_id = db_conn_id
        self.conn = conn
        self.conn_str = None
        self.conn_type = None


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
            if self.conn_type not in self.SCHEMAS:
                raise AirflowException('Could not create Petl connection for connection type {}'.format(self.conn_type))
            logging.info('Establishing connection to {}'.format(self.db_conn_id))
        return self.conn


class CursorProxy(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def executemany(self, statement, parameters, **kwargs):
        # convert parameters to a list
        parameters = list(parameters)
        return self._cursor.executemany(statement, parameters, **kwargs)

    def __getattr__(self, item):
        return getattr(self._cursor, item)


#class PetlReadOperator(BaseOperator):
#    template_fields = ('csv_path', 'db_fields', 'db_sql')
#    ui_color = '#88ccff'
#
#    @apply_defaults
#    def __init__(self,
#                 db_conn_id,
#                 csv_path,
#                 db_table_name=None,
#                 db_fields=None,
#                 db_sql=None,
#                 *args, **kwargs):
#        super(PetlReadOperator, self).__init__(*args, **kwargs)
#        self.db_conn_id = db_conn_id
#        self.db_table_name = db_table_name
#        self.csv_path = csv_path
#        self.db_fields = db_fields
#        self.db_sql = db_sql
#
#    def get_cursor(self, cur):
#        return CursorProxy(cur)
#
#    def execute(self, context):
#        logging.info("Connecting to the database {}".format(self.db_conn_id))
#        self.hook = PetlHook(db_conn_id=self.db_conn_id)
#        self.conn = self.hook.get_conn()
#        self.cursor = self.conn.cursor()
#        if self.db_sql:
#            rows = etl.fromdb(self.conn, self.db_sql)
#            rows.tocsv(self.csv_path)
#        elif self.db_table_name:
#            print("here...")
##            rows = etl.fromdb(self.conn, 'select {self.db_field_overrides} from {self.db_table_name}'.format(self.db_field_overrides, self.db_table_name) if self.db_fields_override else 'select * from {self.db_table_name}'.format(self.db_table_name))
##            rows.tocsv(self.csv_path)
#        else:
#            print("table name or sql stmt required...")
#            raise
#
#        logging.info("Done!")


class PetlWriteOperator(BaseOperator):
    template_fields = ('db_table_name','csv_path', 'append')
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
                 append=False,
                 *args, **kwargs):
        super(PetlWriteOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        print(self.db_table_name)
        self.csv_path = csv_path
        self.db_table_where = db_table_where
        self.db_field_overrides = db_field_overrides or {}
        self.sql_override = sql_override
        self.append = append

    def get_cursor(self, cur):
        return CursorProxy(cur)

    def execute(self, context):
        logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = PetlHook(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()
        self.cursor = self.conn.cursor()
        if self.hook.conn_type == 'postgres':
            rows = etl.fromcsv(self.csv_path)
            rows.todb(self.conn, self.db_table_name)
        elif self.hook.conn_type == 'oracle':
            self.cursor.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
         " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
         " NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
            rows = etl.fromcsv(self.csv_path)
            header = [field_name.upper() for field_name in rows[0]]
            rows = rows.setheader(header)
            print(etl.look(rows))
            if self.append:
                etl.appenddb(rows, self.get_cursor(self.cursor), self.db_table_name)
            else:
                etl.todb(rows, self.get_cursor(self.cursor), self.db_table_name)

        logging.info("Done!")



class PetlPlugin(AirflowPlugin):
    name = "petl_plugin"
    operators = [PetlWriteOperator,]
#    operators = [PetlReadOperator,PetlWriteOperator]
    hooks = [PetlHook]
