#!/usr/bin/env python3.5

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


class BatchDatabridgeTask():

    def __init__(self, task_name, **kwargs):
        self.task_name = task_name
        self.db_type = kwargs.get('db_type', '')
        self.db_host = kwargs.get('db_host', '')
        self.db_user = kwargs.get('db_user', '')
        self.db_password = kwargs.get('db_password', '')
        self.db_name = kwargs.get('db_name', '')
        self.db_port = kwargs.get('db_port', '')
        self.db_connection_string = kwargs.get('db_connection_string', '')
        self.db_table_schema = kwargs.get('db_table_schema', '').upper()
        self.db_table_name = kwargs.get('db_table_name', '').upper()
        self.db_timestamp=kwargs.get('db_timestamp', True)
        self.hash_field = kwargs.get('hash_field', 'etl_hash')
        self.s3_bucket = kwargs.get('s3_bucket', '')
        self.conn = ''  # DB conn
        self.sql = '' # Carto connn
        self._logger = None
        self.CARTO_USR_BASE_URL = "https://{user}.carto.com/"
        self.json_schema_file = kwargs.get('json_schema_file', '')
        self.db_indexes_fields = kwargs.get('db_indexes_fields')
        self.db_select_users = kwargs.get('db_select_users')
        self.schema_fmt = ''
        self.geom_field = ''
        self.geom_srid = ''
        self.num_rows_in_upload_file = None
        self.temp_table_name = 't_' + self.db_table_name
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
        if self.db_table_schema is None:
            db_table_schema = self.json_schema_file.split('__')[0].split('_')[1]
            if db_table_schema[0].isdigit():
                db_table_schema = '_' + db_table_schema
            self.db_table_schema = db_table_schema
        return self.db_table_schema

    @property
    def db_table_name(self):
        if self.db_table_name is None:
            db_table_name = self.json_schema_file.split('__')[1].split('.')[0]
            self.db_table_name = self.table_prefix + '_' + db_table_name
        return self.db_table_name

    @property
    def db_schema_table_name(self):
        return '{}.{}'.format(self.db_table_schema, self.db_table_name)

    @property
    def s3_key(self):
        return 'staging/{}/{}.csv'.format(self.db_table_schema, self.db_table_name)

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
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(self.logger.INFO)
           sh = self.logger.StreamHandler(sys.stdout)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger 

    def load_to_s3(self):
       self.logger.info('Starting load to s3: {}'.format(self.s3_key))
       s3 = boto3.resource('s3') 
       s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))
       self.logger.info('Successfully loaded to s3: {}'.format(self.s3_key))
       
    def get_from_s3(self):
        self.logger.info('Fetching s3://{}/{}'.format(self.s3_bucket, self.s3_key))
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.s3_key).download_file(self.csv_path)
        self.logger.info('s3://{}/{} successfully downloaded'.format(self.s3_bucket, self.s3_key))

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
        self.load_to_s3()
        os.remove(self.csv_path)
        self.logger.info('Successfully extracted from {}: {}'.format(self.db_name, self.db_schema_table_name))

    def write(self):
        self.logger.info('Starting write to {}: {}'.format(self.db_name, self.db_schema_table_name))
        self.make_connection()
        self.get_from_s3()

        if self.db_type == 'postgres':
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
            rows.topostgis(self.conn, self.db_schema_table_name)

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
        if not (self.account and self.password):
            creds = re.match(carto_connection_string_regex, self.connection_string).groups()
            self.account = creds[0]
            self.password = creds[1]
        self.logger.info("Making connection to Carto {} account...".format(self.account))
        try:
            auth_client = APIKeyAuthClient(api_key=self.password, base_url=self.CARTO_USR_BASE_URL.format(user=self.account))
            self.sql = SQLClient(auth_client)
        except Exception as e:
            self.logger.error("failed making connection to Carto {} account...".format(self.account))
            raise e

    def carto_sql_call(self, stmt, log_response=False):
        self.logger.info("Executing: {}".format(stmt))
        response = self.sql.send(stmt)
        if log_response:
            self.logger.info(response)
        return response

    def carto_format_schema(self):
        if not self.json_schema_file:
            self.logger.error('json schema file is required...')
            raise
        with open(self.json_schema_file) as json_file:
            schema = json.load(json_file).get('fields', '')
            if not schema:
                self.logger.error('json schema malformatted...')
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
        self.logger.info('{} - creating table indexes - {}'.format(table_name=self.temp_table_name,
                                                               indexes_fields=self.db_indexes_fields))
        stmt = ''
        for indexes_field in self.db_indexes_fields:
            stmt += 'CREATE INDEX {table}_{field} ON "{table}" ("{field}");\n'.format(table=self.temp_table_name,
                                                                                      field=indexes_field)
        self.carto_sql_call(stmt)

    def carto_create_table(self):
        self.format_schema()
        stmt = ''' CREATE TABLE {table_name} ({schema})'''.format(table_name=self.temp_table_name,
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

    def carto_get_geom_field(self):
        with open(self.json_schema_file) as json_file:
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
        self.get_from_s3()
        self.logger.info("CSV PATH: ", self.csv_path)
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
        self.carto_get_geom_field()
        if self.geom_field and self.geom_srid:
            rows = rows.convert(self.geom_field,
                                lambda c: 'SRID={srid};{geom}'.format(srid=self.geom_srid, geom=c) if c else '')
            write_file = self.csv_path.replace('.csv', '_t.csv')
            rows.tocsv(write_file)
        else:
            write_file = self.csv_path
        self.logger.info("write_file: ", write_file)
        q = "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(
            table_name=self.temp_table_name, header=str_header)
        url = self.CARTO_USR_BASE_URL.format(user=self.account) + 'api/v2/sql/copyfrom'
        with open(write_file, 'rb') as f:
            r = requests.post(url, params={'api_key': self.password, 'q': q}, data=f, stream=True)

            if r.status_code != 200:
                self.logger.info("response: ", r.text)
            else:
                status = r.json()
                self.logger.info("Success: %s rows imported" % status['total_rows'])

    def carto_verify_count(self):
        data = self.carto_sql_call('SELECT count(*) FROM "{}";'.format(self.temp_table_name))
        num_rows_in_table = data['rows'][0]['count']
        num_rows_inserted = num_rows_in_table  # for now until inserts/upserts are implemented
        num_rows_expected = self.num_rows_in_upload_file
        message = '{} - row count - expected: {} inserted: {} '.format(
            self.temp_table_name,
            num_rows_expected,
            num_rows_inserted
        )
        self.logger.info(message)
        if num_rows_in_table != num_rows_expected:
            self.logger.info('Did not insert all rows, reverting...')
            stmt = 'BEGIN;' + \
                   'DROP TABLE if exists "{}" cascade;'.format(self.temp_table_name) + \
                   'COMMIT;'
            self.carto_sql_call(stmt)
            exit(1)

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
        self.logger.info('{} - cdb_cartodbfytable\'ing table'.format(self.temp_table_name))
        self.carto_sql_call("select cdb_cartodbfytable('{}', '{}');".format(self.account, self.temp_table_name))

    def carto_vacuum_analyze(self):
        self.logger.info('{} - vacuum analyzing table'.format(self.temp_table_name))
        self.carto_sql_call('VACUUM ANALYZE "{}";'.format(self.temp_table_name))

    def carto_swap_table(self):
        stmt = 'BEGIN;' + \
               'ALTER TABLE "{}" RENAME TO "{}_old";'.format(self.db_table_name, self.db_table_name) + \
               'ALTER TABLE "{}" RENAME TO "{}";'.format(self.temp_table_name, self.db_table_name) + \
               'DROP TABLE "{}_old" cascade;'.format(self.db_table_name) + \
               self.generate_select_grants() + \
               'COMMIT;'
        self.logger.info("Swapping tables...")
        self.logger.info(stmt)
        self.carto_sql_call(stmt)

    def carto_cleanup(self):
        print("Attempting to drop any temporary tables: {}".format(self.temp_table_name))
        stmt = '''DROP TABLE if exists {} cascade'''.format(self.temp_table_name)
        self.carto_sql_call(stmt)

    def carto_update_table(self):
        self.logger.info("Connecting to the Carto DB account {}".format(self.db_conn_id))
        # Make Carto api sql connection:
        self.carto_make_connection()
        self.temp_table_name = 't_' + self.db_table_name
        try:
            # Create temp table:
            self.logger.info("creating temp table...")
            self.carto_create_table()
            # Write rows to temp table:
            self.logger.info("writing to temp table...")
            self.carto_write()
            # Verify row count:
            self.logger.info("verifying row count...")
            self.carto_verify_count()
            # Cartodbfytable:
            self.logger.info("cartodbfying table...")
            self.carto_cartodbfytable()
            # Create indexes:
            if self.db_indexes_fields:
                self.logger.info("creating indexes...{}".format(self.db_indexes_fields))
                self.carto_create_indexes()
            # Vacuum analyze:
                self.logger.info("vacuum analyzing...")
            self.carto_vacuum_analyze()
            # Swap tables:
            self.logger.info("swapping tables...")
            self.carto_swap_table()
            self.logger.info("Done!")
        except Exception as e:
            self.logger.error("workflow failed, reverting...")
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
