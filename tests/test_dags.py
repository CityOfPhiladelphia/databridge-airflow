import pytest

from airflow.models import DagBag


@pytest.fixture
def dagbag():
    dagbag = DagBag()
    return dagbag

def test_import_dags(dagbag):
    errors = dagbag.import_errors
    assert not errors
