import os
import csv
import mysql.connector as sql
import psycopg2
import petl as etl
from airflow.hooks.base_hook import BaseHook


def source_connection(source_creds):
    """
    connect to source MySQL DB
    """
    src_cnx = sql.connect(user=source_creds.login,
                        password=source_creds.password,
                        host=source_creds.host,
                        database=source_creds.schema)

    if src_cnx.is_connected():
        print('Source connection made:{}'.format(src_cnx))

    return src_cnx


def target_connection(target_creds):
    """
    connect to target Postgres DB
    """
    try:
        tgt_cnx = psycopg2.connect(database=target_creds.schema,
                            user=target_creds.login,
                            password=target_creds.password,
                            host=target_creds.host)

        if tgt_cnx.closed == 0:
            print('Target connection made:{}'.format(tgt_cnx))

        #tgt_cursor = tgt_cnx.cursor()

    except psycopg2.DatabaseError as err:
        print('Failed to connect to target DB')
        raise err

    return tgt_cnx


def extract(source_view, cursor, csv_path):
    """
    Extract view and save to csv
    """
    source_stmt = f'''select * from {source_view}'''
    cursor.execute(source_stmt)
    result = cursor.fetchall()
    header = [h.lower() for h in cursor.column_names]
    #
    with open(csv_path, 'w', newline='', encoding='utf-8') as fp:
        csv_file = csv.writer(fp)
        csv_file.writerows(result)
    #
#    # create dataframe
#    dict_rows = []
#    for row in result:
#        dict_row = dict(zip(header, row))
#        dict_rows.append(dict_row)
#    rows = etl.fromdicts(dict_rows)
#    print(etl.look(rows))

    # create header string
    str_header = ''
    num_fields = len(header)
#    num_rows_in_upload_file = rows.nrows()
    for i, field in enumerate(header):
        if i < num_fields - 1:
            str_header += field + ', '
        else:
            str_header += field

#    # write to temp csv
#    rows.tocsv(csv_path, encoding='utf-8')

    return str_header


def load(target_table_name, cursor, csv_path, str_header):
    """
    Load csv to target db
    """
    print("writing to db...")
    target_table = f'campaign_finance_opd.{target_table_name}'
    print(f"truncating {target_table}...")
    truncate_stmt = f'''truncate table {target_table}'''
    cursor.execute(truncate_stmt)
    print(f"writing to {target_table}...")
    with open(csv_path, 'r', encoding='utf-8') as f:
        with cursor as cursor:
            copy_stmt = "COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true)".format(
                table_name=target_table, header=str_header)
            cursor.copy_expert(copy_stmt, f)


def run_etl(source_conn_id, target_conn_id, views_and_tables):
    """
    Extract and load 
    """
    for source_target_dict in views_and_tables:
        source_creds = BaseHook.get_connection(source_conn_id)
        target_creds = BaseHook.get_connection(target_conn_id)
        source_conn = source_connection(source_creds)
        target_conn = target_connection(target_creds)
        source_view = source_target_dict['source_view']
        target_table = source_target_dict['target_table']

        print("extracting source view: ", source_view)
        temp_csv = 'source_view.csv'
        str_header = extract(source_view, source_conn.cursor(), temp_csv)
        source_conn.close()

        print("Loading csv to target_table: ", target_table)
        load(target_table, target_conn.cursor(), temp_csv, str_header)
        target_conn.commit()
        target_conn.close()
        os.remove(temp_csv)
#        if target_table == 'transaction_data':
#            raise

