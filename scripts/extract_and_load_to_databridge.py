import sys
import os

import petl as etl
import geopetl
import psycopg2
import cx_Oracle
import boto3


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
        self.db_schema_table_name = "{}.{}".format(self.db_table_schema, self.db_table_name)
        self.db_timestamp=kwargs.get('db_timestamp', True)
        self.hash_field = kwargs.get('hash_field', 'etl_hash')
        self.s3_bucket = kwargs.get('s3_bucket', '')
        self.conn = ''

    @property
    def csv_path(self):
        # On Windows, save to a relative folder
        if os.name == 'nt':
            csv_path = 'tmp/' + self.db_table_schema +  '_' + self.db_table_name + '.csv'
        # On Linux, save to absolute tmp folder
        else:
            csv_path = '/tmp/' + self.db_table_schema +  '_' + self.db_table_name + '.csv'

    def load_to_s3(self):
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.csv_path).put(Body=open(self.csv_path, 'rb'))

    def get_from_s3(self):
        s3 = boto3.resource('s3')
        s3.Object(self.s3_bucket, self.csv_path).download_file(self.csv_path)


    def make_connection(self):
        print("Connecting to the database {}".format(self.db_name))
        if self.db_type == 'oracle':
            self.dsn = cx_Oracle.makedsn(self.db_host, self.db_port, self.db_name)
            self.conn = cx_Oracle.connect(user=self.db_user, password=self.db_password, dsn=self.dsn)
        elif self.db_type == 'postgres':
            self.conn = psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_password,
                                         host=self.db_host, port=self.db_port)


    def extract(self):
        try:
            self.make_connection()
        except Exception as e:
            print("Couldn't connect to {}".format(self.db_name))
            raise e
        if self.db_type == 'oracle':
            etl.fromoraclesde(self.conn, self.db_schema_table_name, timestamp=self.db_timestamp) \
               .tocsv(self.csv_path, encoding='latin-1')
        elif self.db_type == 'postgres':
            etl.frompostgis(self.conn, self.db_schema_table_name) \
               .tocsv(self.csv_path, encoding='latin-1')
        self.load_to_s3()
        ## Try to delete the local file ##
        try:
            os.remove(self.csv_path)
        except OSError as e:  ## if failed, report it back to the user ##
            print("Error: %s - %s." % (e.filename, e.strerror))


    def write(self):
        try:
            self.make_connection()
        except Exception as e:
            print("Couldn't connect to {}".format(self.db_name))
            raise e

        # Retrieve file from s3 bucket:
        self.get_from_s3()

        if self.db_type == 'postgres':
            rows = etl.fromcsv(self.csv_path, encoding='latin-1')
            rows.topostgis(self.conn, self.db_schema_table_name)

        ## Try to delete the local file ##
        try:
            os.remove(self.csv_path)
        except OSError as e:  ## if failed, report it back to the user ##
            print("Error: %s - %s." % (e.filename, e.strerror))


    def update_hash(self):
        try:
            self.make_connection()
        except Exception as e:
            print("Couldn't connect to {}".format(self.db_name))
            raise e
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
        print(hash_fields)
        hash_calc = 'md5(' + ' || '.join(hash_fields_fmt) + ')::uuid'
        print(hash_calc)
        update_hash_stmt = '''
            update {table_name_full} set {hash_field} = {hash_calc}
        '''.format(table_name_full=self.db_schema_table_name, hash_field=self.hash_field, hash_calc=hash_calc)
        cur.execute(update_hash_stmt)


    def update_history(self):
        try:
            self.make_connection()
        except Exception as e:
            print("Couldn't connect to {}".format(self.db_name))
            raise e
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


    def run_task(self):
        task_map = {
            'extract': self.extract(),
            'write': self.write(),
            'update_hash': self.update_hash(),
            'update_history': self.update_history()
        }
        return task_map.get(self.task_name)


def main(task_name, **kwargs):
    batch_databridge_task = BatchDatabridgeTask(task_name, **kwargs)
    batch_databridge_task.run_task()

if __name__ == '__main__':
    task_name = sys.argv[1]
    main(task_name, **dict(arg.split('=') for arg in sys.argv[2:]))
