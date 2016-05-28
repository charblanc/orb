import pytest
from test_marks import requires_ms


@pytest.mark.run(order=1)
@requires_ms
def test_pg_loaded(orb):
    from orb.core.connection_types.sql.mssql import MSSQLConnection
    assert orb.Connection.byName('MSSQL') == MSSQLConnection

@pytest.mark.run(order=1)
@requires_ms
def test_pg_db_sync(orb, ms_db, testing_schema, Comment, TestAllColumns):
    ms_db.sync()