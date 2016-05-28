from projex.lazymodule import lazy_import
from ..mssqlconnection import MSSQLStatement

orb = lazy_import('orb')


class ENABLE_INTERNALS(MSSQLStatement):
    def __call__(self, model=None, enabled=True):
        if model is not None:
            sql = u'ALTER TABLE `{0}` {1} KEYS;'.format(model.schema().dbname(), 'ENABLE' if enabled else 'DISABLE')
        else:
            sql = u'SET unique_checks={0};' \
                   u'SET foreign_key_checks={0}'.format(int(enabled))

        return sql, {}


MSSQLStatement.registerAddon('ENABLE INTERNALS', ENABLE_INTERNALS())
