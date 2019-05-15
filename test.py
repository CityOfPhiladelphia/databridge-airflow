import os
from datetime import datetime

import pytest

from airflow.models import DagBag, Connection, TaskInstance
from airflow import DAG, settings

os.environ['ENVIRONMENT'] = "1"
from plugins.carto_operator import S3ToCartoOperator


@pytest.fixture
def dagbag():
    dagbag = DagBag()
    return dagbag

@pytest.fixture
def carto():
    os.environ['AIRFLOW__CORE__FERNET_KEY'] = '46BKJoQYlPPOexq0OhDZnIlNepKFf87WFwLbfzqDDho='
    conn = Connection(
        conn_id='carto_phl',
        conn_type='HTTP',
        login='login',
        password='password',
    )
    session = settings.Session()
    session.add(conn)
    session.commit()

def test_import_dags(dagbag):
    errors = dagbag.import_errors
    assert not errors

# def test_s3_to_carto_operator(carto):
    # with DAG(dag_id='anydag', start_date=datetime.now()) as dag:
    #     task = S3ToCartoOperator(table_schema='schema',
    #                              table_name='table',
    #                              select_users=['user'])
    #     ti = TaskInstance(task=task, execution_date=datetime.now())
    #     result = task.execute(ti.get_template_context())
    #assert result == 