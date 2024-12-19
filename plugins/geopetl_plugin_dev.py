import os
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
encoding='UTF-8'

class GeopetlHookDev(BaseHook):
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
        #print("testestesteset")
        return self.conn_str

    def get_conn(self):
        if self.conn is None:
            params = self.get_connection(self.db_conn_id)
            self.conn_type = params.conn_type
            if self.conn_type == 'oracle' and not params.schema:
                self.dsn = cx_Oracle.makedsn(params.host, params.port, service_name=params.extra)
                self.conn = cx_Oracle.connect(user=params.login, password=params.password, dsn=self.dsn, encoding=encoding, nencoding=encoding)
                return self.conn
            elif self.conn_type == 'postgres':
                self.conn = psycopg2.connect(dbname=params.schema, user=params.login, password=params.password, host=params.host, port=params.port)
                return self.conn
            if self.conn_type not in self.SCHEMAS:
                raise AirflowException('Could not create Geopetl connection for connection type {}'.format(self.conn_type))
            #logging.info('Establishing connection to {}'.format(self.db_conn_id))
        return self.conn


class GeopetlReadOperatorDev(BaseOperator):

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
                 named_version=None,
                 *args, **kwargs):

        super(GeopetlReadOperatorDev, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.csv_path = csv_path
        self.db_table_where = db_table_where
        self.db_field_overrides = db_field_overrides or {}
        self.sql_override = sql_override
        self.db_timestamp = db_timestamp
        self.db_sql = db_sql
        self.named_version = named_version
        #logging.info("extracting to {}".format(self.csv_path))
        #logging.info("plugin where init: {}".format(self.db_table_where))


    def execute(self, context):
        #logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = GeopetlHookDev(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()
        if self.hook.conn_type == 'postgres':
            if self.named_version:
                cur = self.conn.cursor()
                cur.execute("select sde.sde_set_current_version('{}')".format(self.named_version))
            if self.db_sql:
                etl.frompostgis(self.conn, self.db_table_name, sql=self.db_sql).tocsv(self.csv_path)
            else:
                if self.db_table_where:
                    etl.frompostgis(self.conn, self.db_table_name, where=self.db_table_where).tocsv(self.csv_path)
                else:
                    etl.frompostgis(self.conn, self.db_table_name).tocsv(self.csv_path)

        elif self.hook.conn_type == 'oracle':
            if self.db_sql:
                etl.fromoraclesde(self.conn, self.db_table_name, sql=self.db_sql).tocsv(self.csv_path)
            else:
                #logging.info("plugin where: ", self.db_table_where)
                etl.fromoraclesde(self.conn, self.db_table_name, timestamp=self.db_timestamp, where=self.db_table_where).tocsv(self.csv_path)

#        # check for null bytes ('\0') or breaking spaces, and if exists replace with null string (''):
#        has_null_bytes = False
#        with open(self.csv_path, 'r') as infile:
#            for line in infile:
#                for char in line:
#                    if char == '\0' or char == '\xa0':
#                        has_null_bytes_or_breaking_spaces = True
#                        break
#
#        if has_null_bytes_or_breaking_spaces:
#            print("Dataset has null bytes or breaking spaces, removing...")
#            temp_file = self.csv_path.replace('.csv', '_fmt.csv')
#            with open(self.csv_path, 'r') as infile:
#                with open(temp_file, 'w') as outfile:
#                    reader = csv.reader((line.replace('\0', '').replace('\xa0', '') for line in infile), delimiter=",")
#                    writer = csv.writer(outfile)
#                    writer.writerows(reader)
#            os.replace(temp_file, self.csv_path)

        # check for null bytes ('\0') or breaking spaces, and if exists replace with null string (''):

        has_null_bytes = False
        with open(self.csv_path, 'r') as infile:
            for line in infile:
                for char in line:
                    if char == '\0' or char == '\xa0':
                        has_null_bytes = True
                        print(line)
                        print(char)
                        break

        if has_null_bytes:
            print("Dataset has null bytes, removing...")
            temp_file = self.csv_path.replace('.csv', '_fmt.csv')
            with open(self.csv_path, 'r') as infile:
                with open(temp_file, 'w') as outfile:
                    reader = csv.reader((line.replace('\0', '') for line in infile), delimiter=",")
                    writer = csv.writer(outfile)
                    writer.writerows(reader)
            os.replace(temp_file, self.csv_path)

        logging.info("Done!")


class GeopetlWriteOperatorDev(BaseOperator):
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
                 append=False,
                 *args, **kwargs):

        super(GeopetlWriteOperatorDev, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.db_table_name_no_schema = db_table_name.split('.')[1] if '.' in db_table_name else db_table_name
        self.db_table_schema_name = db_table_name.split('.')[0] if '.' in db_table_name else ''
        self.csv_path = csv_path
        self.db_table_where = db_table_where
        self.db_field_overrides = db_field_overrides or {}
        self.sql_override = sql_override
        self.append = append

    def execute(self, context):
        #logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = GeopetlHookDev(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()
        rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        print(etl.look(rows))
        header = [h.lower() for h in rows[0]]
        print('csv_header: ', header)
        rows_fmt = rows.setheader(header)
        print(etl.look(rows_fmt))
        table_fields = []
        cur = self.conn.cursor()
        print('db_table_schema_name: ', self.db_table_schema_name)
        print('db_table_name_no_schema: ', self.db_table_name_no_schema)
        if self.hook.conn_type == 'postgres':
            stmt = f'''select column_name from information_schema.columns where table_schema = '{self.db_table_schema_name}' and table_name = '{self.db_table_name_no_schema}' '''
            print(stmt)
            cur.execute(stmt)
            # Get intersection of table fields and csv fields (header) and write those:
            table_fields = [f[0].lower() for f in cur.fetchall()]
            print('table_fields: ', table_fields)
            table_fields_in_csv = list(set(header) & set(table_fields))
            print('table_fields_in_csv: ', table_fields_in_csv)
            rows_fmt = rows_fmt.cut(table_fields_in_csv)
            print(etl.look(rows_fmt))
            if self.append:
                rows_fmt.appendpostgis(self.conn, self.db_table_name)
            else:
                rows.topostgis(self.conn, self.db_table_name)
        elif self.hook.conn_type == 'oracle':
            cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
         " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
         " NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
            if self.append:
                print('Appending rows...')
                etl.fromcsv(self.csv_path, encoding='utf-8').appendoraclesde(self.conn, self.db_table_name)
            else:
                print('Truncating and appending rows...')
                etl.fromcsv(self.csv_path, encoding='utf-8').tooraclesde(self.conn, self.db_table_name)
        logging.info("Done!")


class GeopetlUpsertOperatorDev(BaseOperator):

    template_fields = ('db_table_name','csv_path','db_table_constraint')
    ui_color = '#88ccff'

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 csv_path,
                 db_table_name,
                 db_table_constraint,
                 version_name=None,
                 *args, **kwargs):

        super(GeopetlUpsertOperatorDev, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.db_table_name = db_table_name
        self.db_table_constraint = db_table_constraint
        self.csv_path = csv_path
        self.version_name = version_name

    def execute(self, context):
        #logging.info("Connecting to the database {}".format(self.db_conn_id))
        self.hook = GeopetlHookDev(db_conn_id=self.db_conn_id)
        self.conn = self.hook.get_conn()
        if self.hook.conn_type == 'postgres':
            rows = etl.fromcsv(self.csv_path)
            if etl.nrows(rows) == 0:
                print("No new geocoded props...")
                self.conn.close()
            else:
                header = rows[0]
                if 'shape' in header:
                    rows = rows.convert('shape', lambda c: None if c in (None, '', 'null') else c)
                placeholders = '%s, ' * (len(header) - 1) + '%s'
                fields_fmt = ', '.join(header)
                upsert_stmt = '''
                INSERT INTO {target_table_name} ({fields_fmt})
                          VALUES ({placeholders})
                ON CONFLICT ON CONSTRAINT {primary_key}
                DO
                UPDATE
                SET ({fields_fmt}) = ({placeholders})
                '''.format(target_table_name=self.db_table_name,
                       fields_fmt=fields_fmt,
                       placeholders=placeholders,
                       primary_key=self.db_table_constraint)

                cur = self.conn.cursor()
                if self.version_name:
                    version_stmt = '''select sde.sde_set_current_version('{}');'''.format(self.version_name)
                    cur.execute(version_stmt)
                for row in rows[1:]:
                    cur.execute(upsert_stmt, row + row)
                self.conn.commit()

        elif self.hook.conn_type == 'oracle':
            print("Geopetl Upsert to Oracle not implemented, exiting...")
            exit(1)
        logging.info("Done!")


class GeopetlPlugin(AirflowPlugin):
    name = "geopetl_plugin"
    operators = [GeopetlReadOperatorDev, GeopetlWriteOperatorDev, GeopetlUpsertOperatorDev]
    hooks = [GeopetlHookDev]
