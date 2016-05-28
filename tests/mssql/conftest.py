import os
import pytest


@pytest.fixture()
def ms_db(request):
    import orb

    host = os.environ.get('ORB_MSSQL_HOST', 'orb.database.windows.net')
    user = os.environ.get('ORB_MSSQL_USER', 'orb_tester@orb')
    pwrd = os.environ.get('ORB_MSSQL_PWRD', '0rbT3st!')

    db = orb.Database('MSSQL')
    db.setName('orb_testing')
    db.setHost(host)
    db.setUsername(user)
    db.setPassword(pwrd)
    db.activate()

    def fin():
        db.disconnect()

    request.addfinalizer(fin)

    return db

@pytest.fixture()
def ms_sql(pg_db):
    import orb
    return orb.Connection.byName('MSSQL')

@pytest.fixture(scope='session')
def ms_all_column_record(orb, TestAllColumns):
    record = TestAllColumns(password='T3st1ng!')
    return record

@pytest.fixture()
def ms_last_column_record(orb, TestAllColumns):
    record = TestAllColumns.select().last()
    return record
