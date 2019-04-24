#!/usr/bin/env python3.5

import csv
import sys
import os
import logging
import json
from operator import methodcaller
import re

import petl as etl
import geopetl
import psycopg2
import cx_Oracle
import boto3
import requests
from carto.sql import SQLClient
from carto.auth import APIKeyAuthClient


# Suppress error thrown by petl on large fields https://stackoverflow.com/questions/15063936/csv-error-field-larger-than-field-limit-131072
csv.field_size_limit(sys.maxsize)
TEST = os.environ.get('TEST', False)

class BatchDatabridgeTask():

    def __init__(self, task_name, **kwargs):
        self._logger = None
        self.task_name = task_name
        self.db_type = kwargs.get('db_type', '')
        self.db_host = kwargs.get('db_host', '')
        self.db_user = kwargs.get('db_user', '')
        self.db_password = kwargs.get('db_password', '')
        self.db_name = kwargs.get('db_name', '')
        self.db_port = kwargs.get('db_port', '')
        self.db_connection_string = kwargs.get('db_connection_string', '')
        self._db_table_schema = kwargs.get('db_table_schema', '')
        self._db_table_name = kwargs.get('db_table_name', '')
        self._temp_table_name = None
        self.db_timestamp=kwargs.get('db_timestamp', True)
        self.hash_field = kwargs.get('hash_field', 'etl_hash')
        self.s3_bucket = kwargs.get('s3_bucket', '')
        self.conn = ''  # DB conn
        self.carto_account = kwargs.get('carto_account', '')
        self.carto_key = kwargs.get('carto_key', '')
        self.carto_connection_string = kwargs.get('carto_connection_string', '')
        self.sql = '' # Carto connn
        self.CARTO_USR_BASE_URL = "https://{user}.carto.com/"
        self.json_schema_file = kwargs.get('json_schema_file', '')
        self.db_indexes_fields = kwargs.get('db_indexes_fields')
        self.db_select_users = kwargs.get('db_select_users')
        self.schema_fmt = ''
        self.geom_field = ''
        self.geom_srid = ''
        self.num_rows_in_upload_file = None
        self.mapping = {
            'string': 'text',
            'number': 'numeric',
            'float': 'numeric',
            'double precision': 'numeric',
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
        self.geom_type_mapping = {
            'point': 'Point',
            'line': 'Linestring',
            'polygon': 'MultiPolygon',
            'multipolygon': 'MultiPolygon',
            'multilinestring': 'MultiLineString'
        }

    @property
    def db_table_schema(self):
        if not self._db_table_schema:
            db_table_schema = self.json_schema_file.split('__')[0].split('_')[1]
            if db_table_schema[0].isdigit():
                db_table_schema = '_' + db_table_schema
            self._db_table_schema = db_table_schema
        return self._db_table_schema

    @property
    def db_table_name(self):
        if not self._db_table_name:
            db_table_name = self.json_schema_file.split('__')[1].split('.')[0]
            self._db_table_name = db_table_name
        return self._db_table_name

    @property
    def db_schema_table_name(self):
        return '{}.{}'.format(self.db_table_schema, self.db_table_name)

    @property
    def databridge_raw_schema_table_name(self):
        # _311
        if self.db_table_schema[0].isdigit():
            return '_{}.databridge_{}'.format(self.db_table_schema, self.db_table_name)
        return '{}.databridge_{}'.format(self.db_table_schema, self.db_table_name)

    @property
    def temp_table_name(self):
        if not self.db_table_name:
            self.logger.error("Can't get table name, exiting...")
            exit(1)
        return 't_' + self.db_table_name

    @property
    def csv_s3_key(self):
        if self.json_schema_file or self.db_name == 'databridge_raw':
            # _311
            if self.db_table_schema[0] == '_':
                return 'staging/{}/{}.csv'.format(self.db_table_schema.split('_')[1], self.db_table_name)
            return 'staging/{}/{}.csv'.format(self.db_table_schema, self.db_table_name)
        # _311
        if self.db_table_schema[0] == '_':
            return 'staging/{}/{}.csv'.format(self.db_schema.split('_', 1)[1].split('_')[1], self.db_table_name)
        return 'staging/{}/{}.csv'.format(self.db_table_schema.split('_')[1], self.db_table_name)

    @property
    def json_schema_s3_key(self):
        return 'schemas/{}'.format(self.json_schema_file)

    @property
    def csv_path(self):
        # On Windows, save to current directory
        if os.name == 'nt':
            csv_path = '{}_{}.csv'.format(self.db_table_schema, self.db_table_name)
        # On Linux, save to tmp folder
        else:
            csv_path = '/tmp/{}_{}.csv'.format(self.db_table_schema, self.db_table_name)
        return csv_path

    @property
    def json_schema_path(self):
        # On windows, save to current directory
        if os.name == 'nt':
            if not os.path.exists('schemas'):
                os.makedirs('schemas')
            json_schema_path = 'schemas/{}'.format(self.json_schema_file)
        else:
            if not os.path.exists('/tmp/schemas'):
                os.makedirs('/tmp/schemas')
            json_schema_path = '/tmp/schemas/{}'.format(self.json_schema_file)
        return json_schema_path

    @property
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger

    def load_csv_to_s3(self):
        self.logger.info('Starting load to s3: {}'.format(self.csv_s3_key))
        s3 = boto3.resource('s3') 
        s3.Object(self.s3_bucket, self.csv_s3_key).put(Body=open(self.csv_path, 'rb'))
        self.logger.info('Successfully loaded to s3: {}'.format(self.csv_s3_key))
       
    def get_csv_from_s3(self):
        self.logger.info('Fetching csv s3://{}/{}'.format(self.s3_bucket, self.csv_s3_key))
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.csv_s3_key).download_file(self.csv_path)
        self.logger.info('CSV successfully downloaded.\n'.format(self.s3_bucket, self.csv_s3_key))

    def get_json_schema_from_s3(self):
        self.logger.info('Fetching json schema: s3://{}/{}'.format(self.s3_bucket, self.json_schema_s3_key))
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.json_schema_s3_key).download_file(self.json_schema_path)
        self.logger.info('Json schema successfully downloaded.\n'.format(self.s3_bucket, self.json_schema_s3_key))

    def make_connection(self):
        self.logger.info('Trying to connect to db_host: {}, db_port: {}, db_name: {}'.format(self.db_host, self.db_port, self.db_name))
        if self.db_type == 'oracle':
            self.dsn = cx_Oracle.makedsn(self.db_host, self.db_port, self.db_name)
            self.logger.info('Made dsn: {}'.format(self.dsn))
            self.conn = cx_Oracle.connect(user=self.db_user, password=self.db_password, dsn=self.dsn)
        elif self.db_type == 'postgres':
            self.conn = psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_password,
                                             host=self.db_host, port=self.db_port)
        self.logger.info('Connected to database {}'.format(self.db_name))

    def extract(self):
        self.logger.info('Starting extract from {}: {}'.format(self.db_name, self.db_schema_table_name))
        self.make_connection()
        if self.db_type == 'oracle':
            etl.fromoraclesde(self.conn, self.db_schema_table_name, timestamp=self.db_timestamp) \
               .tocsv(self.csv_path, encoding='latin-1')
        elif self.db_type == 'postgres':
            etl.frompostgis(self.conn, self.db_schema_table_name) \
               .tocsv(self.csv_path, encoding='latin-1')
        self.load_csv_to_s3()
        os.remove(self.csv_path)
        self.logger.info('Successfully extracted from {}: {}'.format(self.db_name, self.db_schema_table_name))

    def write(self):
        self.logger.info('Starting write to {}: {}'.format(self.db_name, self.databridge_raw_schema_table_name))
        self.make_connection()
        self.get_csv_from_s3()

        if self.db_type == 'postgres':
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
            rows.topostgis(self.conn, self.databridge_raw_schema_table_name)

        self.logger.info('Successfully wrote to {}: {}'.format(self.db_name, self.databridge_raw_schema_table_name))

    def update_hash(self):
        self.logger.info('Starting update hash on {}: {}'.format(self.db_name, self.db_schema_table_name))
        self.make_connection()
        cur = self.conn.cursor()
        hash_fields_stmt = '''
            SELECT array_agg(COLUMN_NAME::text order by COLUMN_NAME)
            FROM information_schema.columns
            WHERE table_schema='{table_schema}' AND table_name='{table_name}'
            and column_name not like 'etl%'
        '''.format(table_schema=self.db_table_schema, table_name=self.db_table_name)
        cur.execute(hash_fields_stmt)
        hash_fields = cur.fetchone()[0]
        hash_fields_fmt = ["COALESCE({}::text,  '')".format(f) for f in hash_fields]
        self.logger.info('Hash fields: {}'.format(hash_fields))
        hash_calc = 'md5(' + ' || '.join(hash_fields_fmt) + ')::uuid'
        self.logger.info('Hash calc: {}'.format(hash_calc))
        update_hash_stmt = '''
            update {table_name_full} set {hash_field} = {hash_calc}
        '''.format(table_name_full=self.db_schema_table_name, hash_field=self.hash_field, hash_calc=hash_calc)
        cur.execute(update_hash_stmt)
        self.logger.info('Sucessfully updated hash {}: {}'.format(self.db_name, self.db_schema_table_name))

    def update_history(self):
        self.logger.info('Starting update history on {}: {}'.format(self.db_name, self.db_schema_table_name))
        self.make_connection()
        cur = self.conn.cursor()
        non_admin_fields_stmt = '''
           SELECT array_agg(COLUMN_NAME::text)
           FROM information_schema.columns
           WHERE table_schema='{table_schema}' AND table_name='{table_name}'
      	   and column_name not like 'etl%'
           '''.format(table_schema=self.table_schema, table_name=self.table_name)
        cur.execute(non_admin_fields_stmt)
        non_admin_fields = cur.fetchone()[0]
        admin_fields = 'etl_read_timestamp, etl_write_timestamp, etl_hash, etl_action'
        insert_fields = non_admin_fields + ', ' + admin_fields
        update_history_stmt = '''
            WITH
            current_hashes as (
                SELECT * FROM (
                SELECT DISTINCT ON ({hash_field}) *
                FROM   {table_schema}.{table_name}_history
                ORDER  BY {hash_field}, etl_action_timestamp DESC NULLS LAST
                ) foo WHERE etl_action != 'delete'
            )
            ,
            computed_new as (
                select {non_admin_fields}, raw.etl_read_timestamp, raw.etl_write_timestamp, raw.{hash_field}
                from {table_schema}.{table_name} raw
                inner join
                (
                    SELECT {hash_field} from {table_schema}.{table_name}
                    EXCEPT
                    select {hash_field} from current_hashes
                ) new_hashes  on new_hashes.{hash_field} = raw.{hash_field}
            )
            ,
            computed_deleted as (
                select {non_admin_fields}, etl_read_timestamp, etl_write_timestamp, cur.{hash_field}
                from current_hashes cur
                inner join
                (
                    SELECT {hash_field} from current_hashes
                    EXCEPT
                    select {hash_field} from {table_schema}.{table_name}
                ) del_hashes  on del_hashes.{hash_field} = cur.{hash_field}
            )
            ,
            computed_final as (
                SELECT new.*, 'insert' as etl_action from computed_new new
                UNION
                SELECT deleted.*, 'delete' as etl_action from computed_deleted deleted
            )
            INSERT INTO {table_schema}.{table_name}_history ({insert_fields})
            select * from computed_final
            '''.format(hash_field=self.hash_field, non_admin_fields=non_admin_fields, table_schema=self.db_table_schema, table_name=self.db_table_name,
                           insert_fields=insert_fields)

        cur.execute(update_history_stmt)
        self.logger.info('Successfully updated history for {}:{}'.format(self.db_name, self.db_schema_table_name))

    def carto_make_connection(self):
        carto_connection_string_regex = r'^carto://(.+):(.+)'
        if not (self.carto_account and self.carto_key):
            creds = re.match(carto_connection_string_regex, self.carto_connection_string).groups()
            self.carto_account = creds[0]
            self.carto_key = creds[1]
        self.logger.info('Making connection to Carto {} account...'.format(self.carto_account))
        try:
            auth_client = APIKeyAuthClient(api_key=self.carto_key, base_url=self.CARTO_USR_BASE_URL.format(user=self.carto_account))
            self.sql = SQLClient(auth_client)
            self.logger.info('Connected to Carto.\n')
        except Exception as e:
            self.logger.error('Failed making connection to Carto {} account...'.format(self.carto_account))
            raise e

    def carto_sql_call(self, stmt, log_response=False):
        self.logger.info('Executing: {}'.format(stmt))
        response = self.sql.send(stmt)
        if log_response:
            self.logger.info(response)
        return response

    def carto_format_schema(self):
        if not self.json_schema_file:
            self.logger.error('Json schema file is required...')
            raise
        with open(self.json_schema_path) as json_file:
            schema = json.load(json_file).get('fields', '')
            if not schema:
                self.logger.error('Json schema malformatted...')
                raise
            num_fields = len(schema)
            for i, scheme in enumerate(schema):
                scheme_type = self.mapping.get(scheme['type'].lower(), scheme['type'])
                if scheme_type == 'geometry':
                    scheme_srid = scheme.get('srid', '')
                    scheme_geometry_type = self.geom_type_mapping.get(scheme.get('geometry_type', '').lower(), '')
                    if scheme_srid and scheme_geometry_type:
                        scheme_type = '''geometry ({}, {}) '''.format(scheme_geometry_type, scheme_srid)
                    else:
                        self.logger.error('srid and geometry_type must be provided with geometry field...')
                        raise

                self.schema_fmt += ' {} {}'.format(scheme['name'], scheme_type)
                if i < num_fields - 1:
                    self.schema_fmt += ','

    def carto_create_indexes(self):
        self.logger.info('Creating indexes on {}: {}'.format(table_name=self.temp_table_name,
                                                             indexes_fields=self.db_indexes_fields))
        stmt = ''
        for indexes_field in self.db_indexes_fields:
            stmt += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=self.temp_table_name,
                                                                                      field=indexes_field)
        self.carto_sql_call(stmt)
        self.logger.info('Indexes created successfully.\n')

    def carto_create_table(self):
        self.logger.info('Creating temp Carto table...')
        self.carto_format_schema()
        stmt = '''DROP TABLE IF EXISTS {table_name}; 
                  CREATE TABLE {table_name} ({schema});'''.format(table_name=self.temp_table_name,
                                                                  schema=self.schema_fmt)
        self.carto_sql_call(stmt)
        check_table_sql = "SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = '{}');"
        response = self.carto_sql_call(check_table_sql.format(self.temp_table_name))
        exists = response['rows'][0]['exists']

        if not exists:
            message = '{} - Could not create table'.format(self.temp_table_name)
            self.logger.error(message)
            raise Exception(message)

        if self.db_indexes_fields:
            self.logger.info("Indexing fields: {}".format(self.db_indexes_fields))
            self.create_indexes()

        self.logger.info('Temp table created successfully.\n')

    def carto_get_geom_field(self):
        with open(self.json_schema_path) as json_file:
            schema = json.load(json_file).get('fields', '')
            if not schema:
                self.logger.error('json schema malformatted...')
                raise
            for scheme in schema:
                scheme_type = self.mapping.get(scheme['type'].lower(), scheme['type'])
                if scheme_type == 'geometry':
                    self.geom_srid = scheme.get('srid', '')
                    self.geom_field = scheme.get('name', '')

    def carto_write(self):
        self.get_csv_from_s3()
        rows = etl.fromcsv(self.csv_path, encoding='latin-1') \
                  .cutout('etl_read_timestamp')
        header = rows[0]
        str_header = ''
        num_fields = len(header)
        self.num_rows_in_upload_file = rows.nrows()
        for i, field in enumerate(header):
            if i < num_fields - 1:
                str_header += field + ', '
            else:
                str_header += field

        self.logger.info('Writing to temp table...')
        # format geom field:
        self.carto_get_geom_field()
        if self.geom_field and self.geom_srid:
            rows = rows.convert(self.geom_field,
                                lambda c: 'SRID={srid};{geom}'.format(srid=self.geom_srid, geom=c) if c else '')
        write_file = self.csv_path.replace('.csv', '_t.csv')
        rows.tocsv(write_file)
        q = "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(
            table_name=self.temp_table_name, header=str_header)
        url = self.CARTO_USR_BASE_URL.format(user=self.carto_account) + 'api/v2/sql/copyfrom'
        with open(write_file, 'rb') as f:
            r = requests.post(url, params={'api_key': self.carto_key, 'q': q}, data=f, stream=True)

            if r.status_code != 200:
                self.logger.error('Carto Write Error Response: {}'.format(r.text))
                self.logger.error('Exiting...')
                exit(1)
            else:
                status = r.json()
                self.logger.info('Carto Write Successful: {} rows imported.\n'.format(status['total_rows']))

    def carto_verify_count(self):
        self.logger.info('Verifying Carto row count...')
        data = self.carto_sql_call('SELECT count(*) FROM "{}";'.format(self.temp_table_name))
        num_rows_in_table = data['rows'][0]['count']
        num_rows_inserted = num_rows_in_table  # for now until inserts/upserts are implemented
        num_rows_expected = self.num_rows_in_upload_file
        message = '{} - expected rows: {} inserted rows: {}.'.format(
            self.temp_table_name,
            num_rows_expected,
            num_rows_inserted
        )
        self.logger.info(message)
        if num_rows_in_table != num_rows_expected:
            self.logger.error('Did not insert all rows, reverting...')
            stmt = 'BEGIN;' + \
                   'DROP TABLE if exists "{}" cascade;'.format(self.temp_table_name) + \
                   'COMMIT;'
            self.carto_sql_call(stmt)
            exit(1)
        self.logger.info('Row count verified.\n')

    def carto_generate_select_grants(self):
        grants_sql = ''
        if not self.db_select_users:
            return grants_sql
        for user in self.db_select_users:
            self.logger.info('{} - Granting SELECT to {}'.format(self.db_table_name, user))
            grants_sql += 'GRANT SELECT ON "{}" TO "{}";'.format(self.db_table_name, user)
        self.logger.info(grants_sql)
        return grants_sql

    def carto_cartodbfytable(self):
        self.logger.info('Cartodbfytable\'ing table: {}'.format(self.temp_table_name))
        self.carto_sql_call("select cdb_cartodbfytable('{}', '{}');".format(self.carto_account, self.temp_table_name))
        self.logger.info('Successfully Cartodbyfty\'d table.\n')

    def carto_vacuum_analyze(self):
        self.logger.info('Vacuum analyzing table: {}'.format(self.temp_table_name))
        self.carto_sql_call('VACUUM ANALYZE "{}";'.format(self.temp_table_name))
        self.logger.info('Vacuum analyze complete.\n')

    def carto_swap_table(self):
        stmt = 'BEGIN;' + \
               'ALTER TABLE "{}" RENAME TO "{}_old";'.format(self.db_table_name, self.db_table_name) + \
               'ALTER TABLE "{}" RENAME TO "{}";'.format(self.temp_table_name, self.db_table_name) + \
               'DROP TABLE "{}_old" cascade;'.format(self.db_table_name) + \
               self.carto_generate_select_grants() + \
               'COMMIT;'
        self.logger.info('Swapping temporary and production tables...')
        self.logger.info(stmt)
        self.carto_sql_call(stmt)

    def carto_cleanup(self):
        self.logger.info('Attempting to drop any temporary tables: {}'.format(self.temp_table_name))
        stmt = '''DROP TABLE if exists {} cascade'''.format(self.temp_table_name)
        self.carto_sql_call(stmt)
        self.logger.info('Temporary tables dropped successfully.\n')

    def carto_update_table(self):
        if TEST:
            self.logger.info('THIS IS A TEST RUN, PRODUCTION TABLES WILL NOT BE AFFECTED!\n')
        self.get_json_schema_from_s3()
        try:
            self.carto_make_connection()
            self.carto_create_table()
            self.carto_write()
            self.carto_verify_count()
            self.carto_cartodbfytable()
            if self.db_indexes_fields:
                self.carto_create_indexes()
            self.carto_vacuum_analyze()
            if TEST:
                self.carto_cleanup()
            else:
                self.carto_swap_table()
            self.logger.info('Done!')
        except Exception as e:
            self.logger.error('Workflow failed, reverting...')
            self.carto_cleanup()
            raise e

    def run_task(self):
        return methodcaller(self.task_name)(self)

def main(task_name, **kwargs):
    batch_databridge_task = BatchDatabridgeTask(task_name, **kwargs)
    batch_databridge_task.run_task()

if __name__ == '__main__':
    task_name = sys.argv[1]
    main(task_name, **dict(arg.split('=') for arg in sys.argv[2:]))
