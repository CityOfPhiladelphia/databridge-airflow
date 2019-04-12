#!/usr/bin/env python3.5

import sys
import os
import logging
from operator import methodcaller

import petl as etl
import geopetl
import psycopg2
import cx_Oracle
import boto3
from botocore.exceptions import ClientError


class BatchDatabridgeTask():

    def __init__(self, task_name, **kwargs):
        self.task_name = task_name
        self.db_type = kwargs.get('db_type', '')
        self.db_host = kwargs.get('db_host', '')
        self.db_user = kwargs.get('db_user', '')
        self.db_password = kwargs.get('db_password', '')
        self.db_name = kwargs.get('db_name', '')
        self.db_port = kwargs.get('db_port', '')
        self.db_table_schema = kwargs.get('db_table_schema', '').upper()
        self.db_table_name = kwargs.get('db_table_name', '').upper()
        self.db_timestamp=kwargs.get('db_timestamp', True)
        self.hash_field = kwargs.get('hash_field', 'etl_hash')
        self.s3_bucket = kwargs.get('s3_bucket', '')
        self.conn = ''
        self._logger = None

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
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           formatter = logging.Formatter('%(levelname)s - %(message)s')
           sh.setFormatter(formatter)
           logger.addHandler(sh)
           self._logger = logger
       return self._logger 

    def load_to_s3(self):
       self.logger.info('Starting load to s3: {}'.format(self.s3_key))
       s3 = boto3.resource('s3') 
       try:
            s3.Object(self.s3_bucket, self.s3_key).put(Body=open(self.csv_path, 'rb'))
            self.logger.info('Successfully loaded to s3: {}'.format(self.s3_key))
       except Exception as e:
            self.logger.exception('Error loading to s3')
    def get_from_s3(self):
        self.logger.info('Fetching s3://{}/{}'.format(self.s3_bucket, self.s3_key))
        s3 = boto3.resource('s3')
        try:
            s3.Object(self.s3_bucket, self.s3_key).download_file(self.csv_path)
            self.logger.info('s3://{}/{} successfully downloaded'.format(self.s3_bucket, self.s3_key))
        except ClientError as e:
            self.logger.exception('No s3 object found: s3://{}/{}'.format(self.s3_bucket, self.s3_key))

    def make_connection(self):
        self.logger.info('Trying to connect to db_host: {}, db_port: {}, db_name: {}'.format(self.db_host, self.db_port, self.db_name))
        try:
            if self.db_type == 'oracle':
                self.dsn = cx_Oracle.makedsn(self.db_host, self.db_port, self.db_name)
                self.logger.info('Made dsn: {}'.format(self.dsn))
                self.conn = cx_Oracle.connect(user=self.db_user, password=self.db_password, dsn=self.dsn)
            elif self.db_type == 'postgres':
                self.conn = psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_password,
                                             host=self.db_host, port=self.db_port)
            self.logger.info('Connected to database {}'.format(self.db_name))

        except cx_Oracle.DatabaseError as e:
            self.logger.exception('Could not connect to database {}'.format(self.db_name))
    def extract(self):
        self.logger.info('Starting extract from {}: {}'.format(self.db_name, self.db_schema_table_name))
        try:
            self.make_connection()
            if self.db_type == 'oracle':
                etl.fromoraclesde(self.conn, self.db_schema_table_name, timestamp=self.db_timestamp) \
                   .tocsv(self.csv_path, encoding='latin-1')
            elif self.db_type == 'postgres':
                etl.frompostgis(self.conn, self.db_schema_table_name) \
                   .tocsv(self.csv_path, encoding='latin-1')
            self.load_to_s3()
            os.remove(self.csv_path)
        except OSError as e:
            self.logger.exception('Error removing temporary file {} - {}'.format(e.filename, e.strerror))
        except Exception as e:
            self.logger.exception('Error extracting from {}: {}'.format(self.db_name, self.db_schema_table_name))
    def write(self):
        self.logger.info('Starting write to {}: {}'.format(self.db_name, self.db_schema_table_name))
        try:
            self.make_connection()
            self.get_from_s3()

            if self.db_type == 'postgres':
                rows = etl.fromcsv(self.csv_path, encoding='latin-1')
                rows.topostgis(self.conn, self.db_schema_table_name)

            os.remove(self.csv_path)
            self.logger.info('Successfully wrote to {}: {}'.format(self.db_name, self.db_schema_table_name))
        except OSError as e: 
            self.logger.exception("Error removing temporary file: {} - {}".format(e.filename, e.strerror))
        except Exception as e:
            self.logger.exception('Error writing to {}: {}'.format(self.db_name, self.db_schema_table_name))

    def update_hash(self):
        self.logger.info('Starting update hash on {}: {}'.format(self.db_name, self.db_schema_table_name))
        try:
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
        except Exception as e:
            self.logger.exception('Error updating hash on {}: {}'.format(self.db_name, self.db_schema_table_name))

    def update_history(self):
        self.logger.info('Starting update history on {}: {}'.format(self.db_name, self.db_schema_table_name))
        try:
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
                    ORDER  BY {hash_field}, etl_read_timestamp DESC NULLS LAST
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
        except Exception as e:
            self.logger.exception('Error updating history for {}:{}'.format(self.db_name, self.db_schema_table_name))

    def run_task(self):
        return methodcaller(self.task_name)(self)

def main(task_name, **kwargs):
    batch_databridge_task = BatchDatabridgeTask(task_name, **kwargs)
    batch_databridge_task.run_task()

if __name__ == '__main__':
    task_name = sys.argv[1]
    main(task_name, **dict(arg.split('=') for arg in sys.argv[2:]))
