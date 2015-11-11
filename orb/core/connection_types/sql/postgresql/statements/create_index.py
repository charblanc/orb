from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class CREATE_INDEX(PSQLStatement):
    def __call__(self, index, checkFirst=False):
        """
        Modifies the table to add and remove the given columns.

        :param model: <orb.Model>
        :param add: [<orb.Column>, ..]
        :param remove: [<orb.Column>, ..]

        :return: <bool>
        """
        schema_name = index.schema().dbname()
        index_name = index.dbname()
        cmd = 'CREATE' if not index.unique() else 'CREATE UNIQUE'

        cols = ['"{0}"'.format(col.field()) if col.testFlag(col.Flags.CaseSensitive)
                else 'lower("{0}")'.format(col.field())
                for col in index.columns()]

        cmd = '{0} INDEX "{1}" ON "{2}" ({3})'.format(cmd, index_name, schema_name, ', '.join(cols))

        if checkFirst:
            cmd = """\
            DO $$
            BEGIN
            IF NOT EXISTS (
                SELECT 1
                FROM pg_indexes
                WHERE schemaname = 'public'
                AND indexname = '{0}'
            ) THEN {1};
            END IF;
            END$$;
            """.format(index_name, cmd)
        else:
            cmd += ';'

        return cmd


PSQLStatement.registerAddon('CREATE INDEX', CREATE_INDEX())
