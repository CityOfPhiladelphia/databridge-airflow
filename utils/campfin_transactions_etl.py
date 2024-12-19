import os
import petl as etl
import geopetl
import cx_Oracle
import psycopg2
from airflow.hooks.base_hook import BaseHook


def oracle_conn(target_creds):
    """
    connect to Oracle connection
    """
    encoding='UTF-8'
    dsn = cx_Oracle.makedsn(target_creds.host, target_creds.port, service_name=target_creds.extra)
    cnx = cx_Oracle.connect(user=target_creds.login, password=target_creds.password, dsn=dsn, encoding=encoding, nencoding=encoding)
    print(cnx)

    return cnx


def postgres_conn(source_creds):
    """
    connect to Postgres DB
    """
    try:
        cnx = psycopg2.connect(database=source_creds.extra,
                            user=source_creds.login,
                            password=source_creds.password,
                            host=source_creds.host)

        if cnx.closed == 0:
            print('Postgres connection made:{}'.format(cnx))

        #src_cursor = src_cnx.cursor()

    except psycopg2.DatabaseError as err:
        print('Failed to connect to target DB')
        raise err

    return cnx


def etl_transactions_from_tripoli_to_db(source_conn_id, target_conn_id, source_table, target_table):
    source_creds = BaseHook.get_connection(source_conn_id)
    target_creds = BaseHook.get_connection(target_conn_id)
    source_conn = postgres_conn(source_creds)
    target_conn = oracle_conn(target_creds)
    rows = etl.fromdb(source_conn, f'select * from {source_table}')
    print(etl.look(rows))
    rows.progress(100).tooraclesde(target_conn, target_table)
    print("Completed successfully, exiting...")


def etl_transactions_from_tripoli_to_db2(source_conn_id, target_conn_id, source_table, target_table):
    source_creds = BaseHook.get_connection(source_conn_id)
    target_creds = BaseHook.get_connection(target_conn_id)
    source_conn = postgres_conn(source_creds)
    target_conn = postgres_conn(target_creds)
    rows = etl.fromdb(source_conn, f'select * from {source_table}')
    rows = rows.convert('termination_report', lambda x: 1 if x == True else 0)
    print(etl.look(rows))
    rows.progress(100).topostgis(target_conn, target_table)
    print("Completed successfully, exiting...")

