from airflow.hooks.postgres_hook import PostgresHook

def get_hash_fields(pg_conn_id, table_schema, table_name):
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    hash_fields_stmt = '''
        SELECT array_agg(COLUMN_NAME::text)
        FROM information_schema.columns
        WHERE table_schema='{table_schema}' AND table_name='{table_name}'
        and column_name not like 'etl%'
    '''.format(table_schema=table_schema, table_name=table_name)
    hash_fields =  pg_hook.get_first(hash_fields_stmt)[0]
    return ','.join(hash_fields)


def update_history_table(**kwargs):
    pg_conn_id=kwargs['db_conn_id']
    table_schema=kwargs['table_schema']
    table_name=kwargs['table_name']
    hash_field=kwargs['hash_field']
    pg_hook = PostgresHook(postgres_conn_id=pg_conn_id)
    hash_fields = get_hash_fields(pg_conn_id, table_schema, table_name)
    etl_fields = 'etl_read_timestamp, etl_write_timestamp, etl_hash, etl_action'
    insert_fields = hash_fields + ', ' + etl_fields
    update_history_stmt = '''
        WITH
        current_hashes as (
            SELECT DISTINCT ON ({hash_field}) *
            FROM   {table_schema}.{table_name}_history
            ORDER  BY etl_hash, etl_read_timestamp DESC NULLS LAST, {hash_field}
        )
        ,
        computed_new as (
            select {hash_fields}, raw.etl_read_timestamp, raw.etl_write_timestamp, raw.etl_hash
            from {table_schema}.{table_name} raw
            inner join
            (
                SELECT etl_hash from {table_schema}.{table_name} 
                EXCEPT
                select etl_hash from current_hashes
            ) new_hashes  on new_hashes.etl_hash = raw.etl_hash
        )
        ,
        computed_deleted as (
            select {hash_fields}, etl_read_timestamp, etl_write_timestamp, cur.etl_hash
            from current_hashes cur
            inner join
            (
                SELECT etl_hash from current_hashes
                EXCEPT
                select etl_hash from opa.buildingcodes
            ) del_hashes  on del_hashes.etl_hash = cur.etl_hash
        )
        ,
        computed_final as (
            SELECT new.*, 'insert' as etl_action from computed_new new
            UNION
            SELECT deleted.*, 'delete' as etl_action from computed_deleted deleted
        )
        INSERT INTO {table_schema}.{table_name}_history ({insert_fields})
        select * from computed_final
    '''.format(hash_field=hash_field, hash_fields=hash_fields, table_schema=table_schema, table_name=table_name, insert_fields=insert_fields)
    pg_hook.run(update_history_stmt)
