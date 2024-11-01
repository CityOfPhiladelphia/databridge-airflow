import os
from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.sftp_operator import SFTPOperator
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.hooks import GeopetlHook
from airflow.operators import CartoUpdateOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from airflow.utils.pin_source_address_std import check_address_comps
from airflow.utils.pin_sql_v2 import *
from airflow.utils.campfin_etl import run_etl
from airflow.utils.campfin_transactions_etl import etl_transactions_from_tripoli_to_db
from airflow.utils.campfin_transactions_etl import etl_transactions_from_tripoli_to_db2
from airflow.utils.campfin_load_csvs import etl_load_csvs
from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators import SSHOperator
from airflow.hooks.base_hook import BaseHook
from datetime import datetime, timedelta
from airflow.models import Variable
import petl as etl
from datetime import datetime, timedelta #, timezone
from pytz import timezone 


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 26, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
#    'on_success_callback': slack_success_alert,
    # 'queue': 'bash_queue',  # TODO: Lookup what queue is
    # 'pool': 'backfill',  # TODO: Lookup what pool is
}

pipeline = DAG('etl_campfin_v0', schedule_interval='30 9 * * *', default_args=default_args)

# -----------------------------------------------------------
def delete_temp_file(**kwargs):
    path = kwargs['templates_dict']['csv_path']
    filename = kwargs['templates_dict']['filename']
    os.remove(path + '/' + filename)


def update_postgres(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs['stmt']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.run(stmt)


def extract_from_postgres(templates_dict, **kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    stmt_where=kwargs.get('stmt_where', '')
    table_name=kwargs['table_name']
    update_date_file = templates_dict.get('update_date_csv_path', '')
    if update_date_file and stmt_where:
        last_update_date = etl.fromcsv(update_date_file)[1][0]
        stmt_where=stmt_where.format(last_update_date=last_update_date)
        stmt = stmt + ' ' + stmt_where if last_update_date else stmt
    print(stmt)
    csv_path=templates_dict['csv_path']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    pg_hook.copy_expert("COPY ({stmt}) to STDOUT WITH CSV HEADER".format(stmt=stmt), csv_path)


def write_to_postgres(templates_dict, **kwargs):

    db_conn_id=kwargs['db_conn_id']
    pg_hook = PostgresHook(postgres_conn_id=db_conn_id)
    stmt=kwargs.get('stmt', '')
    table_name=kwargs['table_name']
    csv_path=templates_dict['csv_path']
    temp_csv_path = csv_path.replace('csv', '_temp.csv')

    # Get target table columns:
    target_columns_stmt = f'''select column_name from information_schema.columns where table_name = '{table_name}' '''
    conn = pg_hook.get_conn()
    cur = conn.cursor()
    cur.execute(target_columns_stmt)
    target_columns = [f[0] for f in cur.fetchall()]
    conn.close()

    # Create string header:
    rows = etl.fromcsv(csv_path, encoding='utf-8') 
    header = rows[0]
    str_header = ''
    header_cols_in_table = [f for f in header if f in target_columns]
    num_fields = len(header_cols_in_table)
    for i, field in enumerate(header_cols_in_table):
        if i < num_fields - 1:
            str_header += field + ', '
        else:
            str_header += field
    print(str_header)


    # Check if target table header and csv have same columns:
    header_sorted = sorted(header)
    target_columns_sorted = sorted(target_columns)
    if header_sorted != target_columns_sorted:
        # If different, output temp csv with target table columns only:
        rows.cut(header_cols_in_table).tocsv(temp_csv_path)
        csv_path = temp_csv_path
    stmt_fmt = stmt.format(header=str_header, table_name=table_name)
    pg_hook.copy_expert(stmt_fmt, csv_path)

#
# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)

# ------------------------------------------------------------
# Extract source mysql views
# pass -> TODO: add to airflow
views_and_tables = [
    {
        'source_view':'filer_balances5', 
        'target_table': 'filer_balances'
    }, 
    {
        'source_view': 'TransactionData21', 
        'target_table': 'transaction_data'
    }
]

etl_source_views = PythonOperator(
    task_id='etl_source_views',
    dag=pipeline,
    python_callable=run_etl,
    op_kwargs={'source_conn_id': 'campfin-mysql', 'target_conn_id':'campfin', 'views_and_tables': views_and_tables},
)

#------------------------------------------------------------
# Update (truncate/append) source mysql views in Tripoli campfin db
# pass -> TODO: add to airflow

#------------------------------------------------------------
# Extract source supplemental data tables (currently in gsheets -> move to Databridge)
staging_and_main_tables_dict = {}
etl_run_tests_and_load_csvs = PythonOperator(
    task_id='etl_run_tests_and_load_csvs',
    dag=pipeline,
    python_callable=etl_load_csvs,
    op_kwargs={'source_conn_id': 'campfin', 'staging_and_main_tables_dict': staging_and_main_tables_dict},
)
#------------------------------------------------------------
# Update (truncate/append) source supplemental data tables in Tripoli
# pass -> TODO: add to airflow

#
# --------------------------------------------------------------
# Extract knack managed tables
knack_campfin_creds = BaseHook.get_connection('knack-campfin')
app_id = knack_campfin_creds.login
api_key = knack_campfin_creds.password
candidates_knack_object_id = 1
campaigns_knack_object_id = 2
filer_types_knack_object_id = 3


extract_knack_candidates = BashOperator(
    task_id='extract_knack_candidates',
    bash_command=f'''extract-knack extract-records {app_id} {api_key} {candidates_knack_object_id} ''' + ''' > '{{ ti.xcom_pull("make_staging") }}/knack_candidates.csv' ''',
    dag=pipeline
)

extract_knack_campaigns = BashOperator(
    task_id='extract_knack_campaigns',
    bash_command=f'''extract-knack extract-records {app_id} {api_key} {campaigns_knack_object_id} ''' + ''' > '{{ ti.xcom_pull("make_staging") }}/knack_campaigns.csv' ''',
    dag=pipeline
)

extract_knack_filer_types = BashOperator(
    task_id='extract_knack_filer_types',
    bash_command=f'''extract-knack extract-records {app_id} {api_key} {filer_types_knack_object_id} ''' + ''' > '{{ ti.xcom_pull("make_staging") }}/knack_filer_types.csv' ''',
    dag=pipeline
)

#-------------------------------------------------------------
# Update Tripoli prepared tables from views
#-----------
# filers
filers_stmt = '''
BEGIN;
truncate table filers;
insert into filers (filer_id
,filer_name
,filer_type
,filer_state
,filer_city
,filer_zip
,filer_local_flag
,candidate_name
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,filer_type_review_status
,is_terminated
,filer_local_flag_desc
)
select filer_id
,filer_name
,filer_type
,filer_state
,filer_city
,filer_zip
,filer_local_flag
,candidate_name
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,filer_type_review_status
,is_terminated
,filer_local_flag_desc
from vw_filers;
COMMIT;
'''
update_filers = PythonOperator(
    task_id='update_filers',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'campfin', 'table_schema':'campfin', 'table_name': 'filers', 'stmt': filers_stmt},
)


# transactions
transactions_stmt = '''
BEGIN;
Truncate table transactions;
Insert into transactions (
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,entity_name
,entity_type
,entity_type_detail
,entity_local_flag
,entity_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,entity_id
,entity_name_std
,entity_filer_id
,entity_city
,entity_state
,entity_zip
,entity_zip4
,entity_residency_flag
,entity_naics
,entity_occupation
,entity_employer_name
,entity_employer_address_1
,entity_employer_address_2
,entity_employer_city
,entity_employer_state
,entity_employer_zip
,entity_employer_zip4
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,entity_source_address
,entity_std_address
,filer_type_review_status
,entity_type_review_status
,report_cycle_label
)
select
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,entity_name
,entity_type
,entity_type_detail
,entity_local_flag
,entity_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,entity_id
,entity_name_std
,entity_filer_id
,entity_city
,entity_state
,entity_zip
,entity_zip4
,entity_residency_flag
,entity_naics
,entity_occupation
,entity_employer_name
,entity_employer_address_1
,entity_employer_address_2
,entity_employer_city
,entity_employer_state
,entity_employer_zip
,entity_employer_zip4
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,entity_source_address
,entity_std_address
,filer_type_review_status
,entity_type_review_status
,report_cycle_label
from vw_transactions;
COMMIT;
'''
update_transactions = PythonOperator(
    task_id='update_transactions',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'campfin', 'table_schema':'campfin', 'table_name': 'transactions', 'stmt': transactions_stmt},
)

# this is used for carto update (currently transactions is streamed to Databridge due to an issue with writing the csv that needs troubleshooting)
extract_transactions = GeopetlReadOperator(
   task_id='extract_transactions',
   dag=pipeline,
   csv_path='{{ ti.xcom_pull("make_staging") }}/transactions.csv',
   db_conn_id='campfin',
   db_table_name='campaign_finance_opd.transactions',
   db_table_where='',
)

# balances
balances_stmt = '''
BEGIN;
truncate table balances;
insert into balances (
filing_form_id
,report_id
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,beginning_cash_balance
,ending_cash_balance
,utmz_monetary_contribution
,utmz_in_kind_contribution
,num_contributions
,num_expenditures
,unpaid_debts
,num_paper_filings
,report_year
,election_date
,election_type
,filer_id
,filer_state
,filer_city
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,report_cycle_code
,report_cycle_name
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,report_url
,sum_itmz_monetary_contribution
,sum_itmz_inkind_contribution
,total_monetary_contribution
,total_in_kind_donations
,total_contributions
,total_expenditures
)
select
filing_form_id
,report_id
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,beginning_cash_balance
,ending_cash_balance
,utmz_monetary_contribution
,utmz_in_kind_contribution
,num_contributions
,num_expenditures
,unpaid_debts
,num_paper_filings
,report_year
,election_date
,election_type
,filer_id
,filer_state
,filer_city
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,report_cycle_code
,report_cycle_name
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,report_url
,sum_itmz_monetary_contribution
,sum_itmz_inkind_contribution
,total_monetary_contribution
,total_in_kind_donations
,total_contributions
,total_expenditures
from vw_balances;
COMMIT;
'''
update_balances = PythonOperator(
    task_id='update_balances',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'campfin', 'stmt': balances_stmt},
)

extract_balances = GeopetlReadOperator(
    task_id='extract_balances',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/balances.csv',
    db_conn_id='campfin',
    db_table_name='campaign_finance_opd.balances',
    db_table_where='',
)

# summary
summary_stmt = '''
BEGIN;
truncate table summary;
insert into summary (
filer_id
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,report_year
,filer_state
,filer_city
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,filer_type_review_status
,first_report_id
,first_report_date
,beginning_cash_balance
,last_report_id
,last_report_date
,ending_cash_balance
,ending_unpaid_debts
,sum_itmz_monetary_contribution
,sum_itmz_inkind_contribution
,sum_utmz_monetary_contribution
,sum_utmz_inkind_contribution
,total_monetary_contribution
,total_in_kind_donations
,total_contributions
,total_expenditures
,num_contributions
,num_expenditures
,num_digital_filings
,num_paper_filings
,is_terminated_year
)
select
filer_id
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,report_year
,filer_state
,filer_city
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,filer_type_review_status
,first_report_id
,first_report_date
,beginning_cash_balance
,last_report_id
,last_report_date
,ending_cash_balance
,ending_unpaid_debts
,sum_itmz_monetary_contribution
,sum_itmz_inkind_contribution
,sum_utmz_monetary_contribution
,sum_utmz_inkind_contribution
,total_monetary_contribution
,total_in_kind_donations
,total_contributions
,total_expenditures
,num_contributions
,num_expenditures
,num_digital_filings
,num_paper_filings
,is_terminated_year
from vw_summary;
COMMIT;
'''

update_summary = PythonOperator(
    task_id='update_summary',
    dag=pipeline,
    python_callable=update_postgres,
    op_kwargs={'db_conn_id':'campfin', 'stmt': summary_stmt},
)

extract_summary = GeopetlReadOperator(
    task_id='extract_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/summary.csv',
    db_conn_id='campfin',
    db_table_name='campaign_finance_opd.summary',
    db_table_where='',
)

# contributions
contributions_stmt = '''
BEGIN;
truncate table contributions;
insert into contributions (
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,donor_name
,donor_type
,donor_type_detail
,donor_local_flag
,donor_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,donor_id
,donor_name_std
,donor_filer_id
,donor_city
,donor_state
,donor_zip
,donor_zip4
,donor_residency_flag
,donor_naics
,donor_occupation
,donor_employer_name
,donor_employer_address_1
,donor_employer_address_2
,donor_employer_city
,donor_employer_state
,donor_employer_zip
,donor_employer_zip4
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,donor_source_address
,donor_std_address
,filer_type_review_status
,donor_type_review_status
)
select
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,donor_name
,donor_type
,donor_type_detail
,donor_local_flag
,donor_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,donor_id
,donor_name_std
,donor_filer_id
,donor_city
,donor_state
,donor_zip
,donor_zip4
,donor_residency_flag
,donor_naics
,donor_occupation
,donor_employer_name
,donor_employer_address_1
,donor_employer_address_2
,donor_employer_city
,donor_employer_state
,donor_employer_zip
,donor_employer_zip4
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,donor_source_address
,donor_std_address
,filer_type_review_status
,donor_type_review_status
from vw_contributions;
COMMIT;
'''

#update_contributions = PythonOperator(
#    task_id='update_contributions',
#    dag=pipeline,
#    python_callable=update_postgres,
#    op_kwargs={'db_conn_id':'campfin', 'stmt': contributions_stmt},
#)

extract_contributions = GeopetlReadOperator(
    task_id='extract_contributions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/contributions.csv',
    db_conn_id='campfin',
    db_table_name='campaign_finance_opd.vw_contributions',
    db_table_where='',
)

# expenditures
expenditures_stmt = ''' 
BEGIN;
truncate table expenditures;
insert into expenditures ( 
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,payee_name
,payee_type
,payee_type_detail
,payee_local_flag
,payee_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,payee_id
,payee_name_std
,payee_filer_id
,payee_city
,payee_state
,payee_zip
,payee_zip4
,payee_residency_flag
,payee_naics
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,payee_source_address
,payee_std_address
,filer_type_review_status
,payee_type_review_status
)
select
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,payee_name
,payee_type
,payee_type_detail
,payee_local_flag
,payee_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,payee_id
,payee_name_std
,payee_filer_id
,payee_city
,payee_state
,payee_zip
,payee_zip4
,payee_residency_flag
,payee_naics
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,payee_source_address
,payee_std_address
,filer_type_review_status
,payee_type_review_status
from vw_expenditures;
COMMIT;
'''

#update_expenditures = PythonOperator(
#    task_id='update_expenditures',
#    dag=pipeline,
#    python_callable=update_postgres,
#    op_kwargs={'db_conn_id':'campfin', 'stmt': expenditures_stmt},
#)

extract_expenditures = GeopetlReadOperator(
    task_id='extract_expenditures',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/expenditures.csv',
    db_conn_id='campfin',
    db_table_name='campaign_finance_opd.vw_expenditures',
    db_table_where='',
)




# unpaid_debts
unpaid_debts_stmt = '''
BEGIN;
truncate table unpaid_debts;
insert into unpaid_debts ( 
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,creditor_name
,creditor_type
,creditor_type_detail
,creditor_local_flag
,creditor_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,creditor_id
,creditor_name_std
,creditor_filer_id
,creditor_city
,creditor_state
,creditor_zip
,creditor_zip4
,creditor_residency_flag
,creditor_naics
,creditor_occupation
,creditor_employer_name
,creditor_employer_address_1
,creditor_employer_address_2
,creditor_employer_city
,creditor_employer_state
,creditor_employer_zip
,creditor_employer_zip4
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,creditor_source_address
,creditor_std_address
,filer_type_review_status
,creditor_type_review_status
)
select
transaction_id
,transaction_date
,transaction_type
,transaction_amount
,transaction_description
,description_type
,filer_name
,filer_type
,filer_local_flag
,filer_local_flag_desc
,pa_pac_registry_name
,pa_pac_registry_type
,likely_unaffiliated
,creditor_name
,creditor_type
,creditor_type_detail
,creditor_local_flag
,creditor_local_flag_desc
,report_year
,report_election_date
,election_type
,filer_id
,filer_city
,filer_state
,filer_zip
,filer_actv_cand_flag
,filer_actv_city_cand_flag
,filer_actv_city_cand_desg_flag
,filer_candidate_name
,candidate_office
,office_level
,multiple_offices_flag
,candidate_party
,candidate_district_num
,candidate_district_name
,candidate_incumbency
,candidate_election_outcome
,candidate_endorsement
,creditor_id
,creditor_name_std
,creditor_filer_id
,creditor_city
,creditor_state
,creditor_zip
,creditor_zip4
,creditor_residency_flag
,creditor_naics
,creditor_occupation
,creditor_employer_name
,creditor_employer_address_1
,creditor_employer_address_2
,creditor_employer_city
,creditor_employer_state
,creditor_employer_zip
,creditor_employer_zip4
,report_cycle_code
,report_cycle_name
,report_cycle_start_date
,report_cycle_end_date
,report_cycle_due_date
,report_doc_type
,report_doc_type_detail
,report_type
,report_certified_date
,report_filed_by
,report_filing_office
,report_filing_status
,termination_report
,est_trans_count
,transaction_supertype
,label_total
,filing_form_id
,report_id
,report_url
,creditor_source_address
,creditor_std_address
,filer_type_review_status
,creditor_type_review_status
from vw_unpaid_debts;
COMMIT;
'''

#update_unpaid_debts = PythonOperator(
#    task_id='update_unpaid_debts',
#    dag=pipeline,
#    python_callable=update_postgres,
#    op_kwargs={'db_conn_id':'campfin', 'stmt': unpaid_debts_stmt},
#)

extract_unpaid_debts = GeopetlReadOperator(
    task_id='extract_unpaid_debts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/unpaid_debts.csv',
    db_conn_id='campfin',
    db_table_name='campaign_finance_opd.vw_unpaid_debts',
    db_table_where='',
)

extract_candidate_campaigns = GeopetlReadOperator(
    task_id='extract_candidate_campaigns',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/campaigns.csv',
    db_conn_id='campfin',
    db_table_name='campaign_finance_opd.vw_candidate_campaigns',
    db_table_where='',
)

#---------------------------------------------------------------
# Write to Databridge:
write_csv_to_postgres_stmt = '''
BEGIN;
truncate table {table_name};
COPY {table_name} ({header}) FROM STDIN WITH (FORMAT csv, HEADER true);
COMMIT;
'''
pg_knack_campaigns_table_name = 'campaigns_csv'
write_knack_campaigns = PythonOperator(
    task_id='write_knack_campaigns_to_pg',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'campfin', 'table_name': pg_knack_campaigns_table_name, 'stmt': write_csv_to_postgres_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/knack_campaigns.csv'},
)

pg_knack_candidates_table_name = 'candidates_csv'
write_knack_candidates = PythonOperator(
    task_id='write_knack_candidates_to_pg',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'campfin', 'table_name': pg_knack_candidates_table_name, 'stmt': write_csv_to_postgres_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/knack_candidates.csv'},
)

pg_knack_filer_types_table_name = 'filer_types_csv'
write_knack_filer_types = PythonOperator(
    task_id='write_knack_filer_types_to_pg',
    dag=pipeline,
    python_callable=write_to_postgres,
    provide_context=True,
    op_kwargs={'db_conn_id':'campfin', 'table_name': pg_knack_filer_types_table_name, 'stmt': write_csv_to_postgres_stmt},
    templates_dict={'csv_path':'{{ ti.xcom_pull("make_staging") }}/knack_filer_types.csv'},
)


write_contributions = GeopetlWriteOperator(
    task_id='write_contributions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/contributions.csv',
    db_conn_id='databridge-boe',
    db_table_name='gis_boe.campfin_contributions',
)

write_contributions_v2 = GeopetlWriteOperator(
    task_id='write_contributions_v2',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/contributions.csv',
    db_conn_id='databridge-v2-boe',
    db_table_name='boe.campfin_contributions',
)

write_expenditures = GeopetlWriteOperator(
    task_id='write_expenditures',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/expenditures.csv',
    db_conn_id='databridge-boe',
    db_table_name='gis_boe.campfin_expenditures',
)

write_expenditures_v2 = GeopetlWriteOperator(
    task_id='write_expenditures_v2',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/expenditures.csv',
    db_conn_id='databridge-v2-boe',
    db_table_name='boe.campfin_expenditures',
)

write_unpaid_debts = GeopetlWriteOperator(
    task_id='write_unpaid_debts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/unpaid_debts.csv',
    db_conn_id='databridge-boe',
    db_table_name='gis_boe.campfin_unpaid_debts',
)

write_unpaid_debts_v2 = GeopetlWriteOperator(
    task_id='write_unpaid_debts_v2',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/unpaid_debts.csv',
    db_conn_id='databridge-v2-boe',
    db_table_name='boe.campfin_unpaid_debts',
)

#write_transactions = GeopetlWriteOperator(
#    task_id='write_transactions',
#    dag=pipeline,
#    csv_path='{{ ti.xcom_pull("make_staging") }}/transactions.csv',
#    db_conn_id='databridge-boe',
#    db_table_name='gis_boe.campfin_transactions',
#)

write_balances = GeopetlWriteOperator(
    task_id='write_balances',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/balances.csv',
    db_conn_id='databridge-boe',
    db_table_name='gis_boe.campfin_balances',
)

write_balances_v2 = GeopetlWriteOperator(
    task_id='write_balances_v2',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/balances.csv',
    db_conn_id='databridge-v2-boe',
    db_table_name='boe.campfin_balances',
)

write_summary = GeopetlWriteOperator(
    task_id='write_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/summary.csv',
    db_conn_id='databridge-boe',
    db_table_name='gis_boe.campfin_summary',
)

write_summary_v2 = GeopetlWriteOperator(
    task_id='write_summary_v2',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/summary.csv',
    db_conn_id='databridge-v2-boe',
    db_table_name='boe.campfin_summary',
)

write_candidate_campaigns = GeopetlWriteOperator(
    task_id='write_candidate_campaigns',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/campaigns.csv',
    db_conn_id='databridge-boe',
    db_table_name='gis_boe.candidate_campaigns',
)

write_candidate_campaigns_v2 = GeopetlWriteOperator(
    task_id='write_campaigns_v2',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/campaigns.csv',
    db_conn_id='databridge-v2-boe',
    db_table_name='boe.candidate_campaigns',
)

extract_and_write_transactions = PythonOperator(
    task_id='extract_and_write_transactions',
    dag=pipeline,
    python_callable=etl_transactions_from_tripoli_to_db,
    op_kwargs={'source_conn_id': 'campfin', 'target_conn_id':'databridge-boe', 'source_table': 'campaign_finance_opd.transactions', 'target_table': 'CAMPFIN_TRANSACTIONS'},
)
extract_and_write_transactions_db2 = PythonOperator(
    task_id='extract_and_write_transactions_db2',
    dag=pipeline,
    python_callable=etl_transactions_from_tripoli_to_db2,
    execution_timeout=timedelta(minutes=50),
    op_kwargs={'source_conn_id': 'campfin', 'target_conn_id':'databridge-v2-boe', 'source_table': 'campaign_finance_opd.transactions', 'target_table': 'boe.campfin_transactions'},
)


# -----------------------------------------------------------------
# Refresh larger datasets in s3 opendata bucket

gisscripts = BaseHook.get_connection('gisscripts')
sshhook_instance = SSHHook(remote_host="citygeo-SC3-aws.city.phila.local",
                                        username="gisscripts",
                                        password=gisscripts.password,
                                        )
linuxscripts = BaseHook.get_connection('linux-scripts-aws')
sshhook_instance2 = SSHHook(remote_host=linuxscripts.host,
                                        username=linuxscripts.login,
                                        key_file=linuxscripts.password,
                                        )

# Transactions
refresh_s3_transactions = SSHOperator(
                task_id="refresh_s3_transactions",
                dag=pipeline,
                command='Powershell.exe "C:/scripts/public_s3_access/run.ps1 GIS_BOE.CAMPFIN_TRANSACTIONS -nofgdb"',
                ssh_hook=sshhook_instance,
                )

# Contributions
refresh_s3_contributions = SSHOperator(
                task_id="refresh_s3_contributions",
                dag=pipeline,
                command='Powershell.exe "C:/scripts/public_s3_access/run.ps1 GIS_BOE.CAMPFIN_CONTRIBUTIONS -nofgdb"',
                ssh_hook=sshhook_instance,
                )

# Expenditures
refresh_s3_expenditures = SSHOperator(
                task_id="refresh_s3_expenditures",
                dag=pipeline,
                command='Powershell.exe "C:/scripts/public_s3_access/run.ps1 GIS_BOE.CAMPFIN_EXPENDITURES -nofgdb"',
                ssh_hook=sshhook_instance,
                )

# Unpaid Debts
refresh_s3_unpaid_debts = SSHOperator(
                task_id="refresh_s3_unpaid_debts",
                dag=pipeline,
                command='Powershell.exe "C:/scripts/public_s3_access/run.ps1 GIS_BOE.CAMPFIN_UNPAID_DEBTS -nofgdb -novalidation"',
                ssh_hook=sshhook_instance,
                )


# -----------------------------------------------------------------
# Refresh AGO

# Transactions
refresh_ago_transactions = SSHOperator(
                task_id="refresh_ago_transactions",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CAMPFIN_TRANSACTIONS -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )
# Balances
refresh_ago_balances = SSHOperator(
                task_id="refresh_ago_balances",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CAMPFIN_BALANCES -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )

# Expenditures
refresh_ago_expenditures = SSHOperator(
                task_id="refresh_ago_expenditures",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CAMPFIN_EXPENDITURES -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )

# Contributions
refresh_ago_contributions = SSHOperator(
                task_id="refresh_ago_contributions",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CAMPFIN_CONTRIBUTIONS -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )
# Unpaid Debts
refresh_ago_unpaid_debts = SSHOperator(
                task_id="refresh_ago_unpaid_debts",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CAMPFIN_UNPAID_DEBTS -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )
# Summary
refresh_ago_summary = SSHOperator(
                task_id="refresh_ago_summary",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CAMPFIN_SUMMARY -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )

# CAND_CONTRIBUTIONS_ZIP 
refresh_ago_cand_contributions_zip = SSHOperator(
                task_id="refresh_ago_cand_contributions_zip",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CAND_CONTRIBUTIONS_ZIP -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )

# COMTE_CONTRIBUTIONS_ZIP 
refresh_ago_comte_contributions_zip = SSHOperator(
                task_id="refresh_ago_comte_contributions_zip",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d COMTE_CONTRIBUTIONS_ZIP -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )

# SMALL_CONTRIBUTIONS
refresh_ago_small_contributions = SSHOperator(
                task_id="refresh_ago_small_contributions",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d SMALL_CONTRIBUTIONS -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )

# CANDIDATE_CAMPAIGNS
refresh_ago_candidate_campaigns = SSHOperator(
                task_id="refresh_ago_candidate_campaigns",
                dag=pipeline,
                command='C:/scripts/ago_updater/ago_update.py -d CANDIDATE_CAMPAIGNS -o ago -p public_perms -r',
                ssh_hook=sshhook_instance,
                )

#------------------------------------------------------------------
# Refresh Carto

# Transactions
campfin_transactions_schema = Variable.get('schemas') + 'campfin_transactions.json'
trigger_carto_campfin_transactions_update = CartoUpdateOperator(
    task_id='update_carto_campfin_transactions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/transactions.csv',
    db_conn_id='carto_phl',
    db_table_name='campfin_transactions',
    db_schema_json=campfin_transactions_schema,
    #db_indexes_fields=['parcel_number','pwd_parcel_id'],
    db_select_users=['publicuser', 'tileuser']
)

# Contributions
campfin_contributions_schema = Variable.get('schemas') + 'campfin_contributions.json'
trigger_carto_campfin_contributions_update = CartoUpdateOperator(
    task_id='update_carto_campfin_contributions',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/contributions.csv',
    db_conn_id='carto_phl',
    db_table_name='campfin_contributions',
    db_schema_json=campfin_contributions_schema,
    #db_indexes_fields=['parcel_number','pwd_parcel_id'],
    db_select_users=['publicuser', 'tileuser']
)

# Expenditures
campfin_expenditures_schema = Variable.get('schemas') + 'campfin_expenditures.json'
trigger_carto_campfin_expenditures_update = CartoUpdateOperator(
    task_id='update_carto_campfin_expenditures',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/expenditures.csv',
    db_conn_id='carto_phl',
    db_table_name='campfin_expenditures',
    db_schema_json=campfin_expenditures_schema,
    #db_indexes_fields=['parcel_number','pwd_parcel_id'],
    db_select_users=['publicuser', 'tileuser']
)

# Unpaid Debts
campfin_unpaid_debts_schema = Variable.get('schemas') + 'campfin_unpaid_debts.json'
trigger_carto_campfin_unpaid_debts_update = CartoUpdateOperator(
    task_id='update_carto_campfin_unpaid_debts',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/unpaid_debts.csv',
    db_conn_id='carto_phl',
    db_table_name='campfin_unpaid_debts',
    db_schema_json=campfin_unpaid_debts_schema,
    #db_indexes_fields=['parcel_number','pwd_parcel_id'],
    db_select_users=['publicuser', 'tileuser']
)

# Summary
campfin_summary_schema = Variable.get('schemas') + 'campfin_summary.json'
trigger_carto_summary_update = CartoUpdateOperator(
    task_id='update_carto_summary',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/summary.csv',
    db_conn_id='carto_phl',
    db_table_name='campfin_summary',
    db_schema_json=campfin_summary_schema,
    #db_indexes_fields=['parcel_number','pwd_parcel_id'],
    db_select_users=['publicuser', 'tileuser']
)

# Balances
campfin_balances_schema = Variable.get('schemas') + 'campfin_balances.json'
trigger_carto_balances_update = CartoUpdateOperator(
    task_id='update_carto_balances',
    dag=pipeline,
    csv_path='{{ ti.xcom_pull("make_staging") }}/balances.csv',
    db_conn_id='carto_phl',
    db_table_name='campfin_balances',
    db_schema_json=campfin_balances_schema,
    #db_indexes_fields=['parcel_number','pwd_parcel_id'],
    db_select_users=['publicuser', 'tileuser']
)

# -----------------------------------------------------------------
# Refresh AGO indexes:

# Transactions
refresh_ago_campfin_transaction_indexes = SSHOperator(
                task_id="refresh_ago_campfin_transactions_indexes",
                dag=pipeline,
                command='bash /scripts/databridge_etl_tools_ago_uploads/campfin_indexes.sh ',
                ssh_hook=sshhook_instance2,
                )


# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

make_staging >> extract_knack_candidates >> write_knack_candidates >> etl_run_tests_and_load_csvs
make_staging >> extract_knack_campaigns >> write_knack_campaigns >> etl_run_tests_and_load_csvs
make_staging >> extract_knack_filer_types >> write_knack_filer_types >> etl_run_tests_and_load_csvs
make_staging >> etl_source_views >> update_filers >> update_transactions >> extract_and_write_transactions >> refresh_ago_transactions >> refresh_ago_campfin_transaction_indexes >> cleanup
make_staging >> etl_run_tests_and_load_csvs >> update_filers
make_staging >> etl_run_tests_and_load_csvs >> extract_candidate_campaigns >> write_candidate_campaigns >> refresh_ago_candidate_campaigns >> cleanup
update_transactions >> update_balances >> extract_balances >> write_balances >> refresh_ago_balances >> cleanup
update_transactions >> update_summary >> extract_summary >> write_summary >> refresh_ago_summary >> cleanup
update_transactions >> extract_contributions >> write_contributions >> refresh_ago_contributions >> cleanup
update_transactions >> extract_expenditures >> write_expenditures >> refresh_ago_expenditures >> cleanup
update_transactions >> extract_unpaid_debts >> write_unpaid_debts >> refresh_ago_unpaid_debts >> cleanup
cleanup << refresh_ago_cand_contributions_zip << extract_and_write_transactions
cleanup << refresh_ago_comte_contributions_zip << extract_and_write_transactions
cleanup << refresh_ago_small_contributions << extract_and_write_transactions
cleanup << trigger_carto_campfin_transactions_update << extract_transactions << update_transactions
cleanup << trigger_carto_campfin_contributions_update << extract_contributions
cleanup << trigger_carto_campfin_expenditures_update << extract_expenditures
cleanup << trigger_carto_campfin_unpaid_debts_update << extract_unpaid_debts
cleanup << trigger_carto_summary_update << extract_summary
cleanup << trigger_carto_balances_update << extract_balances
extract_and_write_transactions >> refresh_s3_transactions >> cleanup
write_contributions >> refresh_s3_contributions >> cleanup
write_expenditures >> refresh_s3_expenditures >> cleanup
write_unpaid_debts >> refresh_s3_unpaid_debts >> cleanup

# Adding on writes into DB2 - Roland, 4/13/23
update_transactions.set_downstream(extract_and_write_transactions_db2)
extract_unpaid_debts.set_downstream(write_unpaid_debts_v2)
extract_expenditures.set_downstream(write_expenditures_v2)
extract_contributions.set_downstream(write_contributions_v2)
extract_balances.set_downstream(write_balances_v2)
extract_candidate_campaigns.set_downstream(write_candidate_campaigns_v2)
extract_summary.set_downstream(write_summary_v2)
