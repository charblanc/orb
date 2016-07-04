""" Defines the backend connection class for PostgreSQL databases. """

import logging
import orb
import re
import traceback

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
        return True

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

        with native.cursor(as_dict=True) as cursor:
            log.debug('***********************')
            log.debug(command % data)
            log.debug('***********************')

            try:
                rowcount = 0
                for cmd in command.split(';'):
                    cmd = cmd.strip()
                    if cmd:
                        cursor.execute(cmd.strip(';') + ';', data)
                        rowcount += cursor.rowcount

            # look for a disconnection error
            except pymssql.InterfaceError:
                raise orb.errors.ConnectionLost()

            # look for integrity errors
            except (pymssql.IntegrityError, pymssql.OperationalError) as err:
                native.rollback()

                # look for a duplicate error
                if err[0] == 1062:
                    raise orb.errors.DuplicateEntryFound(err[1])

                # look for a reference error
                reference_error = re.search('Key .* is still referenced from table ".*"', nstr(err))
                if reference_error:
                    msg = 'Cannot remove this record, it is still being referenced.'
                    raise orb.errors.CannotDelete(msg)

                # unknown error
                log.debug(traceback.print_exc())
                raise orb.errors.QueryFailed(command, data, nstr(err))

            # connection has closed underneath the hood
            except pymssql.Error as err:
                native.rollback()
                log.error(traceback.print_exc())
                raise orb.errors.QueryFailed(command, data, nstr(err))

            try:
                raw = cursor.fetchall()
                results = [mapper(record) for record in raw]
            except (pymssql.OperationalError, pymssql.ProgrammingError):
                results = []

            native.close()
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
          WHERE table_type = 'BASE TABLE' AND table_catalog=%(db)s
        """

        col_sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = %(table)s AND table_catalog=%(db)s
        """

        index_sql = """
        SELECT constraint_name
        FROM information_schema.table_constraints
        WHERE table_name = %(table)s AND table_catalog=%(db)s
        """

        tables = [x['table_name'] for x in self.execute(tables_sql, {'db': self.database().name()})[0]]

        output = {}
        for table in tables:
            if table.endswith('_i18n'):
                continue

            columns, _ = self.execute(col_sql, {'db': self.database().name(), 'table': table})
            columns = [c['column_name'] for c in columns]

            indexes, _ = self.execute(index_sql, {'db': self.database().name(), 'table': table})
            indexes = [i['constraint_name'] for i in indexes]

            if (table + '_i18n') in tables:
                i18n_columns, _ = self.execute(col_sql, {'db': self.database().name(), 'table': table + '_i18n'})
                columns += [c['column_name'] for c in i18n_columns]

                i18n_indexes, _ = self.execute(index_sql, {'db': self.database().name(), 'table': table + '_i18n'})
                indexes += [i['constraint_name'] for i in i18n_indexes]

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

