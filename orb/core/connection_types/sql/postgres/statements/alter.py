from projex.lazymodule import lazy_import
from ..psqlconnection import PSQLStatement

orb = lazy_import('orb')


class ALTER(PSQLStatement):
    def __call__(self, model, add=None, remove=None, owner='postgres'):
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
            if col.testFlag(col.Flags.I18n):
                add_i18n.append(col)
            else:
                add_standard.append(col)

        # add standard columns
        if add_standard:
            sql_options = {
                'type': typ,
                'name': model.schema().dbname(),
                'fields': u'\t' + ',\n\t'.join([ADD_COLUMN(col)[0] for col in add_standard])
            }
            sql = (
                u'ALTER {type} "{name}"\n'
                u'{fields};'
            ).format(**sql_options)
        else:
            sql = ''

        # add i18n columns
        if add_i18n:
            id_column = model.schema().idColumn()
            id_type = id_column.dbType('Postgres')

            i18n_options = {
                'table': model.schema().dbname(),
                'fields': u'\t' + ',\n\t'.join([ADD_COLUMN(col)[0] for col in add_i18n]),
                'owner': owner,
                'id_type': id_type
            }

            i18n_sql = (
                u'CREATE TABLE IF NOT EXISTS "{table}_i18n" (\n'
                u'  "locale" CHARACTER VARYING(5),\n'
                u'  "{table}_id" {id_type} REFERENCES "{table}" ("id") ON DELETE CASCADE,\n'
                u'  CONSTRAINT "{table}_i18n_pkey" PRIMARY KEY ("locale", "{table}_id")\n'
                u') WITH (OIDS=FALSE);'
                u'ALTER TABLE "{table}_i18n" OWNER TO "{owner}";'
                u'ALTER TABLE "{table}_i18n"'
                u'{fields};'
            ).format(**i18n_options)

            sql += '\n' + i18n_sql

        return sql, {}


PSQLStatement.registerAddon('ALTER', ALTER())