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

requires_pg = pytest.mark.skipif(os.environ.get('ORB_PG_TEST_DISABLED') != 'True' and pg is None, reason='psycopg2 required for Postgres')
requires_mysql = pytest.mark.skipif(os.environ.get('ORB_MYSQL_TEST_DISABLED') != 'True' and mysql is None, reason='PyMySQL required for MySQL')
requires_lite = pytest.mark.skipif(os.environ.get('ORB_SQLITE_TEST_DISABLED') != 'True' and sqlite is None, reason='sqlite3 required for SQLite')
requires_ms = pytest.mark.skipif(os.environ.get('ORB_MSSQL_TEST_DISABLED') != 'True' and mssql is None, reason='pymssql required for ODBC')
