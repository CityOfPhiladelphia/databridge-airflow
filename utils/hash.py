from airflow.hooks.postgres_hook import PostgresHook

def update_hash_fields(**kwargs):
    
    print('KWARGS: ', kwargs)
    pg_conn_id=kwargs['db_conn_id']
    table_schema=kwargs['table_schema']
    table_name=kwargs['table_name']
    hash_field=kwargs['hash_field']
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    hash_fields_stmt = '''
        SELECT array_agg(COLUMN_NAME::text order by COLUMN_NAME)
        FROM information_schema.columns
        WHERE table_schema='{table_schema}' AND table_name='{table_name}'
        and column_name not like 'etl%'
    '''.format(table_schema=table_schema, table_name=table_name)
    hash_fields = pg_hook.get_records(hash_fields_stmt)[0][0]
    hash_fields_fmt = ["COALESCE(' + f + '::text,  '')" for f in hash_fields]
    #print(hash_fields)
    hash_calc = 'md5(' + ' || '.join(hash_fields_fmt) + ')::uuid'
    print(hash_calc)
    update_stmt = '''
    update {table_name_full} set {hash_field} = {hash_calc}
    '''.format(table_name_full='.'.join([table_schema, table_name]), hash_field=hash_field, hash_calc=hash_calc)
    pg_hook.run(update_stmt)

