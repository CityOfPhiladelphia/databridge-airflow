import os
import sys
import pytest

from airflow.models import DagBag

os.environ['ENVIRONMENT'] = 'TEST'
sys.path.append('../')
from dags.databridge_carto_factory import databridge_carto_dag_factory


@pytest.fixture
def dagbag():
    dagbag = DagBag()
    return dagbag

def test_import_dags(dagbag):
    errors = dagbag.import_errors
    assert not errors

def test_all_dags_loaded(dagbag):
    num_files = NUM_EXAMPLE_DAGS = 18

    for root, dirs, files in os.walk(os.path.join('dags', 'carto_dag_config')):
        num_files += len(files)

    for root, dirs, files in os.walk(os.path.join('dags', 'knack_dag_config')):
        num_files += len(files)

    assert dagbag.size() == num_files
