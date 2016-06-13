import orb
import os
import pytest

try:
    backends = os.environ['ORB_TEST_BACKENDS'].split(',')
except KeyError:
    backends = orb.Connection.addons().keys()

# define the requirements for the different backends
requires_ms = pytest.mark.skipif('MSSQL'not in backends, reason='pymssql required for ODBC')
requires_mysql = pytest.mark.skipif('MySQL' not in backends, reason='PyMySQL required for MySQL')
requires_pg = pytest.mark.skipif('Postgres' not in backends, reason='psycopg2 required for Postgres')
requires_lite = pytest.mark.skipif('SQLite' not in backends, reason='sqlite3 required for SQLite')
