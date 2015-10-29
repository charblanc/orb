import logging
import projex.text

from projex.lazymodule import lazy_import
from projex.enum import enum
from ..column import Column

orb = lazy_import('orb')
log = logging.getLogger(__name__)


class ReferenceColumn(Column):
    """
    The ReferenceColumn class type will allow for relational references between models.


    Usage
    ----

        import orb

        class Comment(orb.Table):
            created_by = orb.RelationColumn(reference='User',
                                            reverse=orb.ReferenceColumn.Reversed(name='commments'))

    """
    RemoveAction = enum(
        'DoNothing',    # 1
        'Cascade',      # 2
        'Block'         # 4
    )

    class Reversed(object):
        def __init__(self, name='', cached=False, timeout=None):
            self.name = name
            self.cached = cached
            self.timeout = timeout

    def __init__(self,
                 reference='',
                 removeAction=RemoveAction.Block,
                 reverse=None,
                 **kwds):
        super(ReferenceColumn, self).__init__(**kwds)

        # store reference options
        self.__reference = reference
        self.__removeAction = removeAction
        self.__reverse = reverse

    def extract(self, value, context=None):
        """
        Extracts the value provided back from the database.

        :param value: <variant>
        :param context: <orb.ContextOptions>

        :return: <variant>
        """
        if isinstance(value, (str, unicode)) and value.startswith('{'):
            try:
                value = projex.text.safe_eval(value)
            except StandardError:
                log.exception('Invalid reference found')
                raise orb.errors.OrbError('Invalid reference found.')

        if isinstance(value, dict):
            cls = self.referenceModel()
            if not cls:
                raise orb.errors.TableNotFound(self.reference())
            else:
                load_event = orb.events.DatabaseLoadedEvent(data=value)
                value = cls(context=context)
                value.onDatabaseLoad(load_event)
            return value
        else:
            return super(ReferenceColumn, self).extract(value, context=context)

    def loadJSON(self, jdata):
        """
        Loads the given JSON information for this column.

        :param jdata: <dict>
        """
        super(ReferenceColumn, self).loadJSON(jdata)

        # load additional information
        self.__reference = jdata.get('reference') or self.__reference
        self.__removeAction = jdata.get('removeAction') or self.__removeAction

        reverse = jdata.get('reverse')
        if reverse:
            self.__reverse = ReferenceColumn.Reversed(**reverse)

    def referenceModel(self):
        """
        Returns the model that this column references.

        :return     <Table> || None
        """
        dbname = self.schema().databaseName() or None
        model = orb.system.model(self.reference(), database=dbname)
        if not model:
            raise orb.errors.TableNotFound(self.reference())
        return model

    def reverseInfo(self):
        """
        Returns the reversal information for this column type, if any.

        :return     <orb.ReferenceColumn.Reversed> || None
        """
        return self.__reverse

# register the column addon
Column.registerAddon('Reference', ReferenceColumn)