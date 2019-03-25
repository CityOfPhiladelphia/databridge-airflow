import sys
import logging
import csv
import pyodbc
from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

csv.field_size_limit(sys.maxsize)


class MsSQLHook(BaseHook):

    def __init__(self, db_conn_id, conn=None):
        self.db_conn_id = db_conn_id
        self.conn = conn
        self.conn_str = None
        self.conn_type = None

    def get_conn(self):
        if self.conn is None:
            params = self.get_connection(self.db_conn_id)
            self.conn_type = params.conn_type
            if self.conn_type != 'mssql':
                raise AirflowException(
                    'Could not create MsSQL connection for connection type {}'.format(self.conn_type))
            db_driver = 'ODBC Driver 17 for SQL Server'
            self.conn = pyodbc.connect(
                '''DRIVER={db_driver};SERVER={host};DATABASE={schema};UID={login};PWD={password}'''.format(db_driver=db_driver, host=params.host, schema=params.schema, login=params.login, password=params.password)
            )
        print(self.conn)
        return self.conn


class MsSQLReadOperator(BaseOperator):
    template_fields = ('db_table_name', 'csv_path',)
    ui_color = '#88ccff'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 db_table_name,
                 csv_path,
                 db_table_where=None,
                 db_field_overrides=None,
                 sql_override=None,
                 db_timestamp=True,
                 *args, **kwargs):
        super(MsSQLReadOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.csv_path = csv_path
        self.db_fields = db_field_overrides or {}
        self.db_table_where = db_table_where
        self.sql_override = sql_override
        self.db_timestamp = db_timestamp

    def execute(self, context):
        logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = MsSQLHook(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()
        # Define cursor:
        self.cur = self.conn.cursor()
        selection = self.fields if self.fields else '*'
        stmt = 'select {selection} from {table}'.format(selection=selection, table=self.db_table_name)
        self.cur.execute(stmt)
        f = open(self.csv_path, 'w')
        for row in self.cur:
            f.write(','.join([str(s) for s in row]))
            f.write('\n')
        logging.info("Done!")


# class MsSQLWriteOperator(BaseOperator):
#     template_fields = ('db_table_name', 'csv_path',)
#     ui_color = '#88ccff'
#
#     @apply_defaults
#     def __init__(self,
#                  db_conn_id,
#                  db_table_name,
#                  csv_path,
#                  db_table_where=None,
#                  db_field_overrides=None,
#                  sql_override=None,
#                  *args, **kwargs):
#         super(GeopetlWriteOperator, self).__init__(*args, **kwargs)
#         self.db_conn_id = db_conn_id
#         self.db_table_name = db_table_name
#         self.csv_path = csv_path
#         self.db_table_where = db_table_where
#         self.db_field_overrides = db_field_overrides or {}
#         self.sql_override = sql_override
#
#     def execute(self, context):
#         logging.info("Connecting to the database {}".format(self.db_conn_id))
#         self.hook = GeopetlHook(db_conn_id=self.db_conn_id)
#         self.conn = self.hook.get_conn()
#         if self.hook.conn_type == 'postgres':
#             rows = etl.fromcsv(self.csv_path)
#             #            try:
#             rows.topostgis(self.conn, self.db_table_name)
#         # except Exception as e:
#         #                print(2)
#         #                print(e)
#         #                etl.topostgis(rows, self.conn, self.db_table_name)
#         elif self.hook.conn_type == 'oracle':
#             etl.fromcsv(self.csv_path).tooraclesde(self.conn, self.db_table_name)
#
#         logging.info("Done!")


class MsSQLPlugin(AirflowPlugin):
    name = "mssql_plugin"
    operators = [MsSQLReadOperator]#, MsSQLWriteOperator]
    hooks = [MsSQLHook]

