import pytest
from datetime import datetime

from airflow.models import TaskInstance
from airflow import DAG, settings


# try:
#     from airflow.operators.carto_plugin import S3ToCartoOperator
# except ImportError:
#     from plugins.operators.carto_operator import S3ToCartoOperator


# def test_s3_to_carto_operator(carto):
    # with DAG(dag_id='anydag', start_date=datetime.now()) as dag:
    #     task = S3ToCartoOperator(table_schema='schema',
    #                              table_name='table',
    #                              select_users=['user'])
    #     ti = TaskInstance(task=task, execution_date=datetime.now())
    #     result = task.execute(ti.get_template_context())
    #assert result == 