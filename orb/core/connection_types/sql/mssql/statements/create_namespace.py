from projex.lazymodule import lazy_import
from ..mssqlconnection import MSSQLStatement

orb = lazy_import('orb')


class CREATE_NAMESPACE(MSSQLStatement):
    def __call__(self, namespace):
        """
        Modifies the table to add and remove the given columns.

        :param model: <orb.Model>
        :param add: [<orb.Column>, ..]
        :param remove: [<orb.Column>, ..]

        :return: <bool>
        """
        return 'CREATE SCHEMA IF NOT EXISTS `{0}`'.format(namespace), {}


MSSQLStatement.registerAddon('CREATE NAMESPACE', CREATE_NAMESPACE())
