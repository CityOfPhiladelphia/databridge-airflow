import logging
import sys
import json
from operator import methodcaller
import os

import click


DATA_TYPE_MAP = {
    'string':           'text',
    'number':           'numeric',
    'float':            'numeric',
    'double precision': 'numeric',
    'integer':          'integer',
    'boolean':          'boolean',
    'object':           'jsonb',
    'array':            'jsonb',
    'date':             'date',
    'time':             'time',
    'datetime':         'date',
    'geom':             'geometry',
    'geometry':         'geometry'
}

GEOM_TYPE_MAP = {
    'point':           'Point',
    'line':            'Linestring',
    'polygon':         'MultiPolygon',
    'multipolygon':    'MultiPolygon',
    'multilinestring': 'MultiLineString',
}

class TableGenerator():

    def __init__(self, db_type, db_host, db_user, db_password, db_port, db_name, json_schema_file):
        self.db_type          = db_type
        self.db_host          = db_host
        self.db_user          = db_user
        self.db_password      = db_password
        self.db_port          = db_port
        self.db_name          = db_name
        self.json_schema_file = json_schema_file
        self.table_prefix     = 'databridge'
        self._db_name         = None
        self._db_table_schema = None
        self._db_table_name   = None
        self._db_conn         = None
        self._schema_fmt      = None
        self._logger          = None
    
    @property
    def db_table_schema(self):
        if self._db_table_schema is None:
            db_table_schema = self.json_schema_file.split('__')[0].split('_', 1)[-1]
            self._db_table_schema = db_table_schema
            # Prefix schemas starting with numbers with an _ to avoid errors
            if db_table_schema[0].isdigit():
                self._db_table_schema = '_' + self._db_table_schema
        return self._db_table_schema

    @property
    def db_table_name(self):
        if self._db_table_name is None:
            db_table_name = self.json_schema_file.split('__')[1].split('.')[0]
            self._db_table_name = self.table_prefix + '_' + db_table_name
        return self._db_table_name

    @property
    def db_schema_table_name(self):
        return '{}.{}'.format(self.db_table_schema, self.db_table_name)

    @property
    def conn(self):
        if self._db_conn is None:
            if self.db_type == 'oracle':
                import cx_Oracle
                self.dsn = cx_Oracle.makedsn(self.db_host, self.db_port, self.db_name)
                self.logger.info('Made dsn: {}'.format(self.dsn))
                self._db_conn = cx_Oracle.connect(user=self.db_user, password=self.db_password, dsn=self.dsn)
            elif self.db_type == 'postgres':
                import psycopg2
                self._db_conn = psycopg2.connect(dbname=self.db_name, user=self.db_user, password=self.db_password,
                                                host=self.db_host, port=self.db_port)
        return self._db_conn

    @property
    def schema_fmt(self):
        if self._schema_fmt is None:
            if not self.json_schema_file:
                self.logger.error('Json schema file is required...')
                raise
            with open(self.json_schema_file) as json_file:
                schema = json.load(json_file).get('fields', '')
                if not schema:
                    self.logger.error('Json schema malformatted...')
                    raise
                num_fields = len(schema)
                self._schema_fmt = ''
                for i, scheme in enumerate(schema):
                    scheme_type = DATA_TYPE_MAP.get(scheme['type'].lower(), scheme['type'])
                    constraint = scheme.get('constraints', None)
                    if scheme_type == 'geometry':
                        scheme_srid = scheme.get('srid', '')
                        scheme_geometry_type = GEOM_TYPE_MAP.get(scheme.get('geometry_type', '').lower(), '')
                        if scheme_srid and scheme_geometry_type:
                            scheme_type = 'geometry ({}, {}) '.format(scheme_geometry_type, scheme_srid)
                        else:
                            self.logger.error('Srid and geometry_type must be provided with geometry field...')
                            raise
                    self._schema_fmt += ' {} {}'.format(scheme['name'], scheme_type)
                    if constraint:
                        self._schema_fmt += ' NOT NULL'

                    if i < num_fields - 1:
                        self._schema_fmt += ','
        return self._schema_fmt

    @property
    def logger(self):
       if self._logger is None:
           logger = logging.getLogger(__name__)
           logger.setLevel(logging.INFO)
           sh = logging.StreamHandler(sys.stdout)
           formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
           sh.setFormatter(formatter)  
           logger.addHandler(sh)
           self._logger = logger
       return self._logger 

    def execute_sql(self, stmt):
        with self.conn.cursor() as cursor:
            cursor.execute(stmt)

    def create_insert_trigger(self):
        stmt = '''DROP TRIGGER IF EXISTS insert_{table_name}
                      ON {schema_table_name};
                  CREATE TRIGGER insert_{table_name} 
                      BEFORE INSERT
                      ON {schema_table_name}
                      FOR EACH ROW
                      EXECUTE PROCEDURE public.update_etl_write_timestamp();\n''' \
                .format(table_name=self.db_table_name, schema_table_name=self.db_schema_table_name)
        self.execute_sql(stmt)

    def create_update_trigger(self):
        stmt = '''DROP TRIGGER IF EXISTS update_{table_name}
                      ON {schema_table_name};
                  CREATE TRIGGER update_{table_name} 
                      BEFORE UPDATE
                      ON {schema_table_name}
                      FOR EACH ROW
                      EXECUTE PROCEDURE public.update_etl_write_timestamp();\n''' \
                .format(table_name=self.db_table_name, schema_table_name=self.db_schema_table_name)
        self.execute_sql(stmt)

    def create_table(self):
        stmt = 'CREATE TABLE IF NOT EXISTS {db_schema_table_name} ({schema})'.format(db_schema_table_name=self.db_schema_table_name, schema=self.schema_fmt)
        self.execute_sql(stmt)      

    def execute(self):
        try:
            self.create_table()
            self.create_insert_trigger()
            self.create_update_trigger()
            self.conn.commit()
        except Exception as e:
            self.logger.info('Workflow failed, rolling back...')
            self.conn.rollback()
            raise e
        logging.info('Table {} created successfully with triggers!'.format(self.db_schema_table_name))

@click.command()
@click.option('--db_type')
@click.option('--db_host')
@click.option('--db_user')
@click.option('--db_password')
@click.option('--db_port')
@click.option('--db_name')
@click.option('--json_schema_file', default=None)
def main(db_type, db_host, db_user, db_password, db_port, db_name, json_schema_file):
    # Single
    if json_schema_file:
        table_generator = TableGenerator(
            db_type=db_type, 
            db_host=db_host, 
            db_user=db_user, 
            db_password=db_password, 
            db_port=db_port, 
            db_name=db_name,
            json_schema_file=json_schema_file)
        table_generator.execute()
    # Batch
    else:
        for json_schema_file in os.listdir('schemas'):
            table_generator = TableGenerator(
                db_type=db_type, 
                db_host=db_host, 
                db_user=db_user, 
                db_password=db_password, 
                db_port=db_port, 
                db_name=db_name,
                json_schema_file=os.path.join('schemas', json_schema_file))
            table_generator.execute()

if __name__ == '__main__':
    main()