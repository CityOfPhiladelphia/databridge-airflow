import sys
import re
import json
import logging
import csv
import petl as etl
import geopetl
import cx_Oracle
import psycopg2
import requests
from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient
from airflow.plugins_manager import AirflowPlugin
from airflow.exceptions import AirflowException
from airflow.hooks.base_hook import BaseHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


csv.field_size_limit(sys.maxsize)


mapping = {
        'string': 'text',
        'number': 'numeric',
        'integer': 'integer',
        'boolean': 'boolean',
        'object': 'jsonb',
        'array': 'jsonb',
        'date': 'date',
        'time': 'time',
        'datetime': 'date',
        'geom': 'geometry',
        'geometry': 'geometry'
    }

geom_type_mapping = {
    'point': 'Point',
    'line': 'Linestring',
    'polygon': 'MultiPolygon',
    'multipolygon': 'MultiPolygon',
    'multilinestring': 'MultiLineString'
    }




class CartoHook(BaseHook):

    def __init__(self, db_conn_id, conn=None):
        self.db_conn_id = db_conn_id
        self.conn = conn
        self.conn_str = None
        self.conn_type = None


    def get_conn(self):
        USR_BASE_URL = "https://{user}.carto.com/"
        carto_connection_string_regex = r'^carto://(.+):(.+)'
        params = self.get_connection(self.db_conn_id)
        account = params.login
        api_key = params.password
        connection_string = params.extra

        if not (account and api_key):
            creds = re.match(carto_connection_string_regex, connection_string).groups()
            account = creds[0]
            api_key = creds[1]
        auth_client = APIKeyAuthClient(api_key=api_key, base_url=USR_BASE_URL.format(user=account))
        sql = SQLClient(auth_client)

        return sql, account, api_key, USR_BASE_URL



class CartoUpdateOperator(BaseOperator):

    template_fields = ('db_table_name', 'csv_path')
    ui_color = '#88ccff'

    mapping = {
        'string':          'text',
        'number':          'numeric',
        'float':           'numeric',
        'double precision':  'numeric',
        'integer':         'integer',
        'boolean':         'boolean',
        'object':          'jsonb',
        'array':           'jsonb',
        'date':            'date',
        'time':            'time',
        'datetime':        'date',
        'geom':            'geometry',
        'geometry':        'geometry'
    }

    geom_type_mapping = {
        'point':           'Point',
        'line':            'Linestring',
        'polygon':         'MultiPolygon',
        'multipolygon':    'MultiPolygon',
        'multilinestring': 'MultiLineString'
    }

    @apply_defaults
    def __init__(self,
                 db_conn_id,
                 csv_path,
                 db_table_name,
                 db_schema_json,
#                 db_indexes_fields,
#                 db_select_users,
                 *args, **kwargs):
        super(CartoUpdateOperator, self).__init__(*args, **kwargs)
        self.db_conn_id = db_conn_id
        self.csv_path = csv_path
        self.db_table_name = db_table_name
        self.db_schema_json = db_schema_json
        self.db_indexes_fields = kwargs.get('db_indexes_fields')
        self.db_select_users = kwargs.get('db_select_users')
        self.schema_fmt = ''
        self.geom_field = ''
        self.geom_srid = ''
        self.num_rows_in_upload_file = None
        self.temp_table_name = 't_' + self.db_table_name


    def carto_sql_call(self, stmt, log_response=False):
        print(stmt)
        response = self.sql.send(stmt)
#        print("HEREREREREREeEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEE")
#        print(response)
#        try:
#            print(vars(response))
#        except Exception as e:
#            logging.error('HTTP ' + str(response.status_code) + ': ' + response.text)
#            print("some error occurred", e)
#            raise
        if log_response:
            logging.info(response)
        return response


    def format_schema(self):
        if not self.db_schema_json:
            logging.error('json schema file is required...')
            raise
        with open(self.db_schema_json) as json_file:
            schema = json.load(json_file).get('fields', '')
            if not schema:
                logging.error('json schema malformatted...')
                raise
            num_fields = len(schema)
            for i, scheme in enumerate(schema):
                scheme_type = mapping.get(scheme['type'].lower(), scheme['type'])
                if scheme_type == 'geometry':
                    scheme_srid = scheme.get('srid', '')
                    scheme_geometry_type = geom_type_mapping.get(scheme.get('geometry_type', '').lower(), '')
                    if scheme_srid and scheme_geometry_type:
                        scheme_type = '''geometry ({}, {}) '''.format(scheme_geometry_type, scheme_srid)
                    else:
                        logging.error('srid and geometry_type must be provided with geometry field...')
                        raise

                self.schema_fmt += ' {} {}'.format(scheme['name'], scheme_type)
                if i < num_fields - 1:
                    self.schema_fmt += ','


    def create_indexes(self):
        logging.info('{} - creating table indexes - {}'.format(table_name=self.temp_table_name, indexes_fields=self.db_indexes_fields))
        stmt = ''
        for indexes_field in self.db_indexes_fields:
            stmt += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=table_name, field=indexes_field)
        self.carto_sql_call(stmt)


    def create_table(self):
        self.format_schema()
        stmt = ''' CREATE TABLE {table_name} ({schema})'''.format(table_name=self.temp_table_name, schema=self.schema_fmt)
        self.carto_sql_call(stmt)
        check_table_sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}');"
        response = self.carto_sql_call(check_table_sql.format(self.temp_table_name))
        exists = response['rows'][0]['exists']

        if not exists:
            message = '{} - Could not create table'.format(table_name)
            logging.error(message)
            raise Exception(message)

        if self.db_indexes_fields:
            logging.info("Indexing fields: {}".format(self.db_indexes_fields))
            self.create_indexes()


    def get_geom_field(self):
        with open(self.db_schema_json) as json_file:
            schema = json.load(json_file).get('fields', '')
            if not schema:
                logging.error('json schema malformatted...')
                raise
            for scheme in schema:
                scheme_type = mapping.get(scheme['type'].lower(), scheme['type'])
                if scheme_type == 'geometry':
                    self.geom_srid = scheme.get('srid', '')
                    self.geom_field = scheme.get('name', '')


    def write(self):
        print("CSV PATH: ", self.csv_path)
        rows = etl.fromcsv(self.csv_path, encoding='utf-8')
        # print(etl.look(rows))
        header = rows[0]
        str_header = ''
        num_fields = len(header)
        self.num_rows_in_upload_file = rows.nrows()
        for i, field in enumerate(header):
            if i < num_fields - 1:
                str_header += field + ', '
            else:
                str_header += field

        # format geom field:
        self.get_geom_field()
        if self.geom_field and self.geom_srid:
            rows = rows.convert(self.geom_field, lambda c: 'SRID={srid};{geom}'.format(srid=self.geom_srid, geom=c) if c else '')
            write_file = self.csv_path.replace('.csv', '_t.csv')
            rows.tocsv(write_file)
        else:
            write_file = self.csv_path
        print("write_file: ", write_file)
        q =  "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(table_name=self.temp_table_name, header=str_header)
        url = self.USR_BASE_URL.format(user=self.account) + 'api/v2/sql/copyfrom'
        with open(write_file, 'rb') as f:
            r = requests.post(url, params={'api_key': self.api_key, 'q': q}, data=f, stream=True)

            if r.status_code != 200:
                print("response: ", r.text)
            else:
                status = r.json()
                print("Success: %s rows imported" % status['total_rows'])


    def verify_count(self):
        data = self.carto_sql_call('SELECT count(*) FROM "{}";'.format(self.temp_table_name))
        num_rows_in_table = data['rows'][0]['count']
        num_rows_inserted = num_rows_in_table # for now until inserts/upserts are implemented
        num_rows_expected = self.num_rows_in_upload_file
        message = '{} - row count - expected: {} inserted: {} '.format(
            self.temp_table_name,
            num_rows_expected,
            num_rows_inserted
            )
        logging.info(message)
        if num_rows_in_table != num_rows_expected:
            logging.info('Did not insert all rows, reverting...')
            stmt = 'BEGIN;' + \
              'DROP TABLE if exists "{}" cascade;'.format(self.temp_table_name) + \
              'COMMIT;'
            self.carto_sql_call(stmt)
            exit(1)


    def generate_select_grants(self):
        grants_sql = ''
        if not self.db_select_users:
            return grants_sql
        for user in self.db_select_users:
            logging.info('{} - Granting SELECT to {}'.format(self.db_table_name, user))
            grants_sql += 'GRANT SELECT ON "{}" TO "{}";'.format(self.db_table_name, user)
        print(grants_sql)
        return grants_sql


    def cartodbfytable(self):
        logging.info('{} - cdb_cartodbfytable\'ing table'.format(self.temp_table_name))
        self.carto_sql_call("select cdb_cartodbfytable('{}', '{}');".format(self.account, self.temp_table_name))


    def vacuum_analyze(self):
        logging.info('{} - vacuum analyzing table'.format(self.temp_table_name))
        self.carto_sql_call('VACUUM ANALYZE "{}";'.format(self.temp_table_name))


    def swap_table(self):
        stmt = 'BEGIN;' + \
              'ALTER TABLE "{}" RENAME TO "{}_old";'.format(self.db_table_name, self.db_table_name) + \
              'ALTER TABLE "{}" RENAME TO "{}";'.format(self.temp_table_name, self.db_table_name) + \
              'DROP TABLE "{}_old" cascade;'.format(self.db_table_name) + \
              self.generate_select_grants() + \
              'COMMIT;'
        self.carto_sql_call(stmt)


    def cleanup(self):
        print("Attempting to drop any temporary tables: {}".format(self.temp_table_name))
        stmt = '''DROP TABLE if exists {} cascade'''.format(self.temp_table_name)
        self.carto_sql_call(stmt)



    def execute(self, context):
        logging.info("Connecting to the database {}".format(self.db_conn_id))
        # Make carto api sql connection:
        self.hook = CartoHook(db_conn_id=self.db_conn_id)
        self.sql, self.account, self.api_key, self.USR_BASE_URL = self.hook.get_conn()
        self.temp_table_name = 't_' + self.db_table_name
        try:
            # Create temp table:
            print("creating temp table...")
            self.create_table()
            # Write rows to temp table:
            print("writing to temp table...")
            self.write()
            # Verify row count:
            print("verifying row count...")
            self.verify_count()
            # Cartodbfytable:
            print("cartodbfying table...")
            self.cartodbfytable()
            # Create indexes: 
            if self.db_indexes_fields:
                print("creating indexes...")
                self.create_indexes()
            # Vacuum analyze:
            print("vacuum analyzing...")
            self.vacuum_analyze()
            # Swap tables:
            print("swapping tables...")
            self.swap_table()
            logging.info("Done!")
        except Exception as e:
            print("workflow failed, reverting...")
            self.cleanup()
            raise e


class CartoPlugin(AirflowPlugin):
    name = "carto_plugin"
    operators = [CartoUpdateOperator]
    hooks = [CartoHook]
