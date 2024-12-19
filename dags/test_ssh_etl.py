from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks import GeopetlHook
from airflow.operators import GeopetlReadOperator, GeopetlWriteOperator
from airflow.operators import CreateStagingFolder, DestroyStagingFolder
from airflow.utils.slack import slack_failed_alert, slack_success_alert
from airflow.utils.hash import update_hash_fields
from airflow.utils.history import update_history_table
from datetime import datetime, timedelta


# ============================================================
# Defaults - these arguments apply to all operators

default_args = {
    'owner': 'airflow',  # TODO: Look up what owner is
    'depends_on_past': False,  # TODO: Look up what depends_on_past is
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2022, 2, 4, 0, 0, 0),
    'on_failure_callback': slack_failed_alert,
}

pipeline = DAG('test_ssh_etl', schedule_interval=None, default_args=default_args)


def query_oracle(**kwargs):
    db_conn_id=kwargs['db_conn_id']
    stmt=kwargs.get('stmt', '')
    print(stmt)
    geopetl_hook = GeopetlHook(db_conn_id=db_conn_id)
    conn = geopetl_hook.get_conn()
    cur = conn.cursor()
    cur.execute(stmt)
    conn.commit()

# ------------------------------------------------------------
# Make staging area

make_staging = CreateStagingFolder(
    task_id='make_staging',
    dag=pipeline,
)


# -------------------------------------------------------------
# sync Databridge from AGO
from airflow.contrib.hooks import SSHHook
from airflow.contrib.operators import SSHOperator
from airflow.hooks.base_hook import BaseHook

gisscripts = BaseHook.get_connection('gisscripts')
sshhook_instance = SSHHook(remote_host="citygeo-scripts1.phila.city",
                                        username="gisscripts",
                                        password=gisscripts.password,
                                        )

#------------------------------------------------------------
# Sync Queue 9 review findings from AGO to Databridge:

backwards_q9_review_findings_sync = SSHOperator(
                task_id="backwards_queue_9_review_findings_AGO_to_Databridge",
                dag=pipeline,
                command='E:/arcpy/python.exe E:/Scripts/ago_to_databridge_backup_etl/ago_to_databridge_backup.py -ad "OPA Suspense Queue Findings" -d QUEUE_9_REVIEW_FINDINGS -a gis_cama',
                ssh_hook=sshhook_instance,
                )


#-------------------------------------------------------------

backwards_oracle_sync = SSHOperator(
                task_id="backwards_oracle_sync_AGO_queue_9_to_Databridge",
                dag=pipeline,
                command='E:/arcpy/python.exe E:/Scripts/ago_to_databridge_backup_etl/ago_to_databridge_backup.py -ad PROPERTY_DEEDS_NEW_QUEUE_9 -d PROPERTY_DEEDS_NEW_QUEUE_9_AGO -a gis_cama',
                ssh_hook=sshhook_instance,
                )

# -------------------------------------------------------------
# Update Queue 9 from view

update_opa_property_deeds_new_queue_9_stmt = '''CALL GIS_CAMA.REFRESH_PROP_DEEDS_NEW_QUEUE_9()'''

update_opa_property_deeds_new_queue_9 = PythonOperator(
    task_id='update_opa_property_deeds_new_queue_9',
    dag=pipeline,
    python_callable=query_oracle,
    op_kwargs={'db_conn_id':'databridge-cama', 'stmt': update_opa_property_deeds_new_queue_9_stmt},
)


# -----------------------------------------------------------------
# Refresh AGO

refresh_ago = SSHOperator(
                task_id="refresh_ago",
                dag=pipeline,
                command='E:/arcpy/python.exe E:/Scripts/ago_update_multithread/ago_update.py -d GIS_CAMA_property_deeds_new_queue_9 -o ago -p opa_suspense_queue_review_perms --republish --enable-editing --preserve-editor-tracking',
                ssh_hook=sshhook_instance,
                )

# -----------------------------------------------------------------
# Cleanup - delete staging folder

cleanup = DestroyStagingFolder(
    task_id='cleanup_staging',
    dag=pipeline,
    dir='{{ ti.xcom_pull("make_staging") }}',
)

make_staging >> backwards_oracle_sync >> update_opa_property_deeds_new_queue_9 >> refresh_ago >> cleanup
make_staging >> backwards_q9_review_findings_sync >> cleanup
