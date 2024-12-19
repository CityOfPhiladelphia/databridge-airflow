import petl as etl
import geopetl
import cx_Oracle
import psycopg2
from airflow.hooks.oracle_hook import OracleHook


class CursorProxy(object):
    def __init__(self, cursor):
        self._cursor = cursor

    def executemany(self, statement, parameters, **kwargs):
        # convert parameters to a list
        parameters = list(parameters)
        return self._cursor.executemany(statement, parameters, **kwargs)

    def __getattr__(self, item):
        return getattr(self._cursor, item)


def get_cursor():
    return CursorProxy(dest_conn.cursor())

# Property Deeds New:
def write_new_deeds_for_cama(**kwargs):
    print("writing new records deed data...")
    db_conn_id = kwargs['db_conn_id']
    db_hook = OracleHook(oracle_conn_id=db_conn_id)
    dest_conn = db_hook.get_conn()
    dest_cur = dest_conn.cursor()
    print("Setting database date and timestamp formats...")
    dest_cur.execute("ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS'"
         " NLS_TIMESTAMP_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF'"
         " NLS_TIMESTAMP_TZ_FORMAT = 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'")
    csv_path = kwargs['csv_path']
    print(csv_path)
    destination_table = kwargs['destination_table']
    rows = etl.fromcsv(csv_path, encoding='latin-1')
    print(etl.look(rows))
    etl.todb(rows, get_cursor, destination_table)


# # Property_Homesteads:
# print("writing homestead data...")
# table_name = 'vw_homesteads_for_cama_w_pin'
# rows = etl.frompostgis(source_conn, 'etl_user' + '.' + table_name)
# print(etl.look(rows))
# etl.todb(rows, get_cursor, 'PROPERTY_HOMESTEADS')
#
# # Property_Inspections:
# print("writing inspections data...")
# table_name = 'vw_inspections_for_cama_w_pin'
# rows = etl.frompostgis(source_conn, 'etl_user' + '.' + table_name)
# print(etl.look(rows))
# etl.todb(rows, get_cursor, 'PROPERTY_INSPECTIONS')

# # # Property_PERMITS:
# print("writing permits data...")
# table_name = 'vw_permits_for_cama_w_pin'
# rows = etl.frompostgis(source_conn, 'etl_user' + '.' + table_name)
# print(etl.look(rows))
# etl.todb(rows, get_cursor, 'PROPERTY_PERMITS')

# # Property_Off_Prop_Addresses:
# print("writing off-prop address data...")
# table_name = 'vw_off_prop_address_info_for_cama'
# rows = etl.frompostgis(source_conn, 'etl_user' + '.' + table_name)
# print(etl.look(rows))
# etl.todb(rows, get_cursor, 'PROPERTY_OFF_PROP_ADDRESSES')

