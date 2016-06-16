from projex.lazymodule import lazy_import
from ..mssqlconnection import MSSQLStatement

orb = lazy_import('orb')


class ALTER(MSSQLStatement):
    def __call__(self, model, add=None, remove=None, owner=''):
        """
        Modifies the table to add and remove the given columns.

        :param model: <orb.Model>
        :param add: [<orb.Column>, ..]
        :param remove: [<orb.Column>, ..]

        :return: <bool>
        """
        ADD_COLUMN = self.byName('ADD COLUMN')

        # determine what kind of model we're modifying
        if issubclass(model, orb.Table):
            typ = 'TABLE'
        else:
            raise orb.errors.OrbError('Cannot alter {0}'.format(type(model)))

        # determine the i18n and standard columns
        add_i18n = []
        add_standard = []
        for col in add or []:
            if col.testFlag(col.Flags.Virtual):
                continue

            if col.testFlag(col.Flags.I18n):
                add_i18n.append(col)
            else:
                add_standard.append(col)

        # add standard columns
        if add_standard:
            sql_raw = []
            for col in add_standard:
                sql_raw.append(
                    u'ALTER {type} "{name}"\n'
                    u'{column};'.format(type=typ, name=model.schema().dbname(), column=ADD_COLUMN(col)[0])
                )

            sql = '\n'.join(sql_raw)
        else:
            sql = ''

        # add i18n columns
        if add_i18n:
            id_column = model.schema().idColumn()
            id_type = id_column.dbType('MSSQL')

            i18n_options = {
                'table': model.schema().dbname(),
                'fields': u'\t' + ',\n\t'.join([ADD_COLUMN(col)[0] for col in add_i18n]),
                'owner': owner,
                'id_type': id_type,
                'id_field': id_column.field()
            }

            sql += u"""
            IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='{table}')
            BEGIN
                CREATE TABLE "{table}_i18n" (
                    "locale" VARCHAR(5),
                    "{table}_id" {id_type} REFERENCES "{table}" ("{id_field}") ON DELETE CASCADE,
                    CONSTRAINT "{table}_i18n_pkey" PRIMARY KEY ("locale", "{table}_id")
                )

                ALTER TABLE "{table}_i18n"
                {fields}
            END
            """.format(**i18n_options)

        return sql, {}


MSSQLStatement.registerAddon('ALTER', ALTER())
