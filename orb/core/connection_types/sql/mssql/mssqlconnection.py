""" Defines the backend connection class for PostgreSQL databases. """

import datetime
import logging
import orb
import re

from projex.text import nativestring as nstr

from ..sqlconnection import SQLConnection
from ..sqlstatement import SQLStatement

log = logging.getLogger(__name__)

try:
    import pymssql

except ImportError:
    log.debug('For MSSQL backend, ensure your python version supports sqlite3')
    pymssql = None



class MSSQLStatement(SQLStatement):
    pass


# noinspection PyAbstractClass
class MSSQLConnection(SQLConnection):
    """ 
    Creates a SQL Server backend connection type for handling database
    connections to SQL Server databases.
    """

    # ----------------------------------------------------------------------
    # PROTECTED METHODS
    # ----------------------------------------------------------------------
    def _closed(self, native):
        return False

    def _execute(self,
                 native,
                 command,
                 data=None,
                 returning=True,
                 mapper=dict):
        """
        Executes the inputted command into the current \
        connection cursor.
        
        :param      command    | <str>
                    data       | <dict> || None
                    autoCommit | <bool> | commit database changes immediately
                    autoClose  | <bool> | closes connections immediately
        
        :return     [{<str> key: <variant>, ..}, ..], <int> count
        """
        if data is None:
            data = {}

        # check to make sure the connection hasn't been reset or lost
        cursor = native.cursor()

        # determine if we're executing multiple statements at once
        commands = [cmd for cmd in command.split(';') if cmd]
        if len(commands) > 1:
            commands.insert(0, 'BEGIN TRANSACTION')

        def _gen_sub_value(val):
            output = []
            replace = []

            for sub_value in val:
                if isinstance(sub_value, (list, tuple, set)):
                    cmd, vals = _gen_sub_value(sub_value)
                    replace.append(cmd)
                    output += vals
                else:
                    replace.append('?')
                    output.append(sub_value)

                return '({0})'.format(','.join(replace)), output

        rowcount = 0
        for cmd in commands:
            if not cmd.endswith(';'):
                cmd += ';'

            log.debug('***********************')
            log.debug(command)
            log.debug(data)
            log.debug('***********************')

            try:
                cursor.execute(cmd, data)

                if cursor.rowcount != -1:
                    rowcount += cursor.rowcount

            # look for a cancelled query
            except pymssql.OperationalError as err:
                if err == 'interrupted':
                    raise orb.errors.Interruption()
                else:
                    log.exception('Unkown query error.')
                    raise orb.errors.QueryFailed(cmd, data, nstr(err))

            # look for duplicate entries
            except pymssql.IntegrityError as err:
                duplicate_error = re.search('UNIQUE constraint failed: (.*)', nstr(err))
                if duplicate_error:
                    result = duplicate_error.group(1)
                    msg = '{value} is already being used.'.format(value=result)
                    raise orb.errors.DuplicateEntryFound(msg)
                else:
                    # unknown error
                    log.exception('Unknown query error.')
                    raise orb.errors.QueryFailed(command, data, nstr(err))

            # look for any error
            except Exception as err:
                log.exception('Unknown query error.')
                raise orb.errors.QueryFailed(cmd, data, nstr(err))

        if returning:
            try:
                raw_results = cursor.fetchall()
            except pymssql.OperationalError:
                results = []
            else:
                results = [mapper(record) for record in raw_results]
            rowcount = len(results)  # for some reason, rowcount in mssql3 returns -1 for selects...
        else:
            results = []

        return results, rowcount

    def _open(self, db):
        """
        Handles simple, SQL specific connection creation.  This will not
        have to manage thread information as it is already managed within
        the main open method for the SQLBase class.
        
        :param      db | <orb.Database>
        
        :return     <variant> | backend specific database connection
        """
        if not pymssql:
            raise orb.errors.BackendNotFound('pymssql is not installed.')

        dbname = db.name()

        try:
            mssql_db = pymssql.connect(
                server=db.host(),
                port=db.port() or 1433,
                user=db.username(),
                password=db.password(),
                database=db.name()
            )
            return mssql_db
        except pymssql.Error:
            log.exception('Failed to connect to pymssql')
            raise orb.errors.ConnectionFailed()

    def _interrupt(self, threadId, connection):
        """
        Interrupts the given native connection from a separate thread.
        
        :param      threadId   | <int>
                    connection | <variant> | backend specific database.
        """
        try:
            connection.interrupt()
        except StandardError:
            pass

    def delete(self, records, context):
        count = len(records)
        super(MSSQLConnection, self).delete(records, context)
        return [], count

    def schemaInfo(self, context):
        tables_sql = """
          SELECT table_name
          FROM information_schema.tables
          WHERE table_type = 'BASE TABLE' AND table_catalog='dbName'
        """
        tables = [x['table_name'] for x in self.execute(tables_sql)[0]]
        print tables

        output = {}
        for table in tables:
            if table.endswith('_i18n'):
                continue

            columns, _ = self.execute("PRAGMA table_info({table});".format(table=table))
            columns = [c['name'] for c in columns]

            indexes, _ = self.execute("PRAGMA index_list({table});".format(table=table))
            indexes = [i['name'] for i in indexes]

            if (table + '_i18n') in tables:
                i18n_columns, _ = self.execute("PRAGMA table_info({table}_i18n);".format(table=table))
                columns += [c['name'] for c in i18n_columns]

                i18n_indexes, _ = self.execute("PRAGMA index_list({table}_i18n);".format(table=table))
                indexes += [i['name'] for i in i18n_indexes]

            output[table] = {
                'fields': columns,
                'indexes': indexes
            }

        return output

    # ----------------------------------------------------------------------

    @classmethod
    def statement(cls, code=''):
        """
        Returns the statement interface for this connection.
        
        :param      code | <str>
        
        :return     subclass of <orb.core.backends.sql.SQLStatement>
        """
        return MSSQLStatement.byName(code) if code else MSSQLStatement


# register the SQL Server backend
if pymssql:
    orb.Connection.registerAddon('MSSQL', MSSQLConnection)

