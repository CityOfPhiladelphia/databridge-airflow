import os
import petl as etl
import geopetl
import cx_Oracle
import psycopg2
from airflow.hooks.base_hook import BaseHook





def source_connection(source_creds):
    """
    connect to target Postgres DB
    """
    try:
        src_cnx = psycopg2.connect(database=source_creds.extra,
                            user=source_creds.login,
                            password=source_creds.password,
                            host=source_creds.host)

        if src_cnx.closed == 0:
            print('Target connection made:{}'.format(src_cnx))

        #src_cursor = src_cnx.cursor()

    except psycopg2.DatabaseError as err:
        print('Failed to connect to target DB')
        raise err

    return src_cnx


def run_tests(cur):

    test1_sql = '''
    select count(distinct c.candidate_name) from campaigns_csv p 
    left join candidates_csv c on c.candidate_name = p.candidate_name 
    where  c.candidate_first_name is null
    '''

    test2_sql = '''
        select count(*) from (
            select candidate_name from filer_types_csv where candidate_name is not null and trim(candidate_name) != ''
        except
            select candidate_name from candidates_csv where candidate_name is not null
        ) foo
    '''

    cur.execute(test1_sql)
    val = cur.fetchone()[0]
    if val != 0:
        print("test 1 failed, not upadating tables from csvs...")
        return 1
    else:
        cur.execute(test2_sql)
        val = cur.fetchone()[0]
        if val != 0:
            print("test 2 failed, not updating tables from csvs...")
            return 1
        else:
            return 0 


def etl_load_csvs(source_conn_id, staging_and_main_tables_dict):
    # Create connection:
    source_creds = BaseHook.get_connection(source_conn_id)
    source_conn = source_connection(source_creds)
    cur = source_conn.cursor()

    # Run tests:
    test_status = run_tests(cur)
    if test_status == 1:
        print("Tests failed, exiting...")
        exit(0)
    else:
        print("Tests passed, updating csv tables...")

	# Update csv tables:
        print("Updating filer_types...")

        update_filer_types_stmt = '''
        BEGIN;
        truncate table filer_types;
        insert into filer_types (filer_id
        ,filer_name
        ,filer_type
        ,candidate_name
        ,pa_registry_name
        ,pa_registry_type
        ,likely_unaffiliated
        ,review_status)
        select
        filer_id
        ,filer_name
        ,filer_type
        ,candidate_name
        ,pa_registry_name
        ,pa_registry_type
        ,likely_unaffiliated
        ,review_status
        from filer_types_csv where review_status = 'completed';
        COMMIT;
        '''
        try:
            cur.execute(update_filer_types_stmt)
            source_conn.commit()
        except Exception as e:
            print("Updating filer_types table from csv failed, exiting...")
            raise e

        print("Updating candidates...")

        update_candidates_stmt = '''
        BEGIN;
        truncate table candidates;
        insert into candidates (candidate_name
        ,candidate_name_complete
        ,candidate_first_name
        ,candidate_middle_name
        ,candidate_last_name
        ,candidate_suffix
        ,pa_candidate_id
        ,fec_candidate_id
        ,candidate_nickname
        ,candidate_name_simple
        )
        select distinct
        upper(candidate_name)
        ,upper(candidate_name_complete)
        ,upper(candidate_first_name)
        ,upper(candidate_middle_name)
        ,upper(candidate_last_name)
        ,upper(candidate_suffix)
        ,pa_candidate_id
        ,fec_candidate_id
        ,upper(candidate_nickname)
        ,upper(candidate_name_simple)
        from candidates_csv
        where candidate_name is not null;
        COMMIT;
        '''
        try:
            cur.execute(update_candidates_stmt)
            source_conn.commit()
        except Exception as e:
            print("Updating candidates table from csv failed, exiting...")
            raise e

        print("Updating campaigns...")

        update_campaigns_stmt = '''
        BEGIN;
        truncate table campaigns;
        insert into campaigns (candidate_name
        ,election_year
        ,election_date
        ,election_type
        ,office_sought
        ,party
        ,district_num
        ,district_name
        ,incumbency
        ,election_outcome
        ,endorsement
        ,name_ballot
        ,filer_id_desg_committee
        )
        select
        upper(candidate_name)
        ,election_year
        ,election_date
        ,election_type
        ,office_sought
        ,upper(party)
        ,district_num
        ,district_name
        ,incumbency
        ,election_outcome
        ,endorsement
        ,name_ballot
        ,filer_id_desg_committee
        from campaigns_csv;
        COMMIT;
        '''
        try:
            cur.execute(update_campaigns_stmt)
            source_conn.commit()
        except Exception as e:
            print("Updating campaigns table from csv failed, exiting...")
            raise e

    print("Completed successfully, exiting...")
    source_conn.close()
