from projex.lazymodule import lazy_import
from ..mssqlconnection import MSSQLStatement

orb = lazy_import('orb')


class CREATE(MSSQLStatement):
    def __call__(self, model, owner='', includeReferences=True):
        if issubclass(model, orb.Table):
            return self._createTable(model, owner, includeReferences)
        elif issubclass(model, orb.View):
            return self._createView(model, owner, includeReferences)
        else:
            raise orb.errors.OrbError('Cannot create model for type: '.format(type(model)))

    def _createTable(self, model, owner, includeReferences):
        ADD_COLUMN = self.byName('ADD COLUMN')

        add_i18n = []
        add_standard = []

        # divide columns between standard and translatable
        for col in model.schema().columns(recurse=False).values():
            if col.testFlag(col.Flags.Virtual):
                continue

            if not includeReferences and isinstance(col, orb.ReferenceColumn):
                continue

            if col.testFlag(col.Flags.I18n):
                add_i18n.append(col)
            else:
                add_standard.append(col)

        # create the standard model
        cmd_body = []
        if add_standard:
            cmd_body += [ADD_COLUMN(col)[0].replace('ADD ', '') for col in add_standard]

        inherits = model.schema().inherits()
        if inherits:
            inherits_model = orb.system.model(inherits)
            if not inherits_model:
                raise orb.errors.ModelNotFound(inherits)

            cmd_body.append('__base_id INTEGER')

        # get the primary column
        id_column = model.schema().idColumn()
        if id_column and not inherits:
            pcol = '"{0}"'.format(id_column.field())
            cmd_body.append('CONSTRAINT "{0}_pkey" PRIMARY KEY ({1})'.format(model.schema().dbname(), pcol))

        body = ',\n\t'.join(cmd_body)
        if body:
            body = '\n\t' + body + '\n'

        cmd = u"""
        IF NOT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name='{table}')
        BEGIN
            CREATE TABLE "{table}" (
                {body}
            )
        """.format(table=model.schema().dbname(), body=body, owner=owner)

        # create the i18n model
        if add_i18n:
            id_column = model.schema().idColumn()
            id_type = id_column.dbType('MSSQL').replace('IDENTITY(1,1)', '').strip()

            i18n_body = ',\n\t'.join([ADD_COLUMN(col)[0].replace('ADD COLUMN ', '') for col in add_i18n])

            cmd += u"""
            CREATE TABLE "{table}_i18n" (
                "locale" VARCHAR(5),
                "{table}_id" {id_type} REFERENCES "{table}" ({pcol}),
                {body},
                CONSTRAINT "{table}_i18n_pkey" PRIMARY KEY ("{table}_id", "locale")
            )
            """.format(table=model.schema().dbname(),
                       id_type=id_type,
                       pcol=pcol,
                       body=i18n_body,
                       owner=owner)
        cmd += 'END'

        return cmd, {}

MSSQLStatement.registerAddon('CREATE', CREATE())