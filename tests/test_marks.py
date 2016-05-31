import os
import pytest

try:
    import psycopg2 as pg
except ImportError:
    pg = None

try:
    import sqlite3 as sqlite
except ImportError:
    sqlite = None

try:
    import pymysql as mysql
except ImportError:
    mysql = None

try:
    import pymssql as mssql
except ImportError:
    mssql = None

try:
    backends = os.environ['ORB_TEST_CONNECTIONS'].split(',')
except KeyError:
    backends = []


requires_ms = pytest.mark.skipif(('MSSQL' in backends or not backends) and mssql is None, reason='pymssql required for ODBC')
requires_mysql = pytest.mark.skipif(('MySQL' in backends or not backends) and mysql is None, reason='PyMySQL required for MySQL')
requires_pg = pytest.mark.skipif(('Postgres' in backends or not backends) and pg is None, reason='psycopg2 required for Postgres')
requires_lite = pytest.mark.skipif(('SQLite' in backends or not backends) and sqlite is None, reason='sqlite3 required for SQLite')
