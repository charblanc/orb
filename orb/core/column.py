""" Defines the meta information for a column within a table schema. """

import logging
import projex.text

from projex.addon import AddonManager
from projex.enum import enum
from projex.lazymodule import lazy_import

log = logging.getLogger(__name__)
orb = lazy_import('orb')


class Column(AddonManager):
    """ Used to define database schema columns when defining Table classes. """
    class Index(object):
        def __init__(self, name='', cached=False, timeout=None):
            self.name = name
            self.cached = cached
            self.timeout = timeout

    TypeMap = {}

    Flags = enum(
        'ReadOnly',
        'Private',
        'Polymorphic',
        'Primary',
        'AutoIncrement',
        'Required',
        'Unique',
        'Encrypted',
        'Searchable',
        'Translatable',
        'CaseSensitive',
        'Virtual',
        'Queryable'
    )

    def __init__(self,
                 name=None,
                 field=None,
                 display=None,
                 getter=None,
                 setter=None,
                 index=None,
                 flags=0,
                 default=None,
                 defaultOrder='asc'):
        # support string based flag definition
        if isinstance(flags, set):
            flag_enum = 0
            for flag in flags:
                flag_enum |= self.Flags(flag)
            flags = flag_enum

        # constructor items
        self.__name = name
        self.__field = field
        self.__display = display
        self.__index = index
        self.__flags = flags
        self.__default = default
        self.__defaultOrder = defaultOrder
        self.__getter = getter
        self.__setter = setter

        # custom options
        self.__schema = None
        self.__timezone = None

    def dbRestore(self, db_value, context=None):
        """
        Converts a stored database value to Python.

        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        return db_value

    def dbStore(self, py_value, context=None):
        """
        Prepares to store this column for the a particular backend database.

        :param backend: <orb.Database>
        :param py_value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        # convert base types to work in the database
        if isinstance(py_value, (list, tuple, set)):
            py_value = tuple((self.dbStore(database, x, context=context) for x in py_value))
        elif isinstance(py_value, orb.Collection):
            py_value = py_value.ids()
        elif isinstance(py_value, orb.Model):
            py_value = py_value.id()

        return py_value

    def dbType(self, connectionType):
        """
        Returns the database object type based on the given connection type.

        :param connectionType:  <str>

        :return: <str>
        """
        return self.TypeMap.get(connectionType, self.TypeMap.get('Default'))

    def default(self):
        """
        Returns the default value for this column to return
        when generating new instances.
        
        :return     <variant>
        """
        if isinstance(self.__default, (str, unicode)):
            return self.valueFromString(self.__default)
        else:
            return self.__default

    def defaultOrder(self):
        """
        Returns the default ordering for this column when sorting.

        :return     <str>
        """
        return self.__defaultOrder

    def display(self):
        """
        Returns the display text for this column.

        :return     <str>
        """
        return self.__display or orb.system.syntax().display(self.__name)

    def extract(self, value, context=None):
        """
        Extracts the database value information during a load.

        :param value: <variant>
        :param context: <orb.Context>

        :return: <variant>
        """
        return value

    def field(self):
        """
        Returns the field name that this column will have inside the database.
                    
        :return     <str>
        """
        return self.__field or orb.system.syntax().field(self.__name, isinstance(self, orb.ReferenceColumn))

    def firstMemberSchema(self, schemas):
        """
        Returns the first schema within the list that this column is a member
        of.
        
        :param      schemas | [<orb.TableSchema>, ..]
        
        :return     <orb.TableSchema> || None
        """
        for schema in schemas:
            if schema.hasColumn(self):
                return schema
        return self.schema()

    def flags(self):
        """
        Returns the flags that have been set for this column.
        
        :return     <Column.Flags>
        """
        return self.__flags

    def getter(self):
        return self.__getter or orb.system.syntax().getter(self.__name)

    def index(self):
        """
        Returns the index information for this column, if any.

        :return:    <orb.Column.Index> || None
        """
        return self.__index

    def isMemberOf(self, schemas):
        """
        Returns whether or not this column is a member of any of the given
        schemas.
        
        :param      schemas | [<orb.TableSchema>, ..] || <orb.TableSchema>
        
        :return     <bool>
        """
        if type(schemas) not in (tuple, list, set):
            schemas = (schemas,)

        for schema in schemas:
            if schema.hasColumn(self):
                return True
        return False

    def isNull(self, value):
        """
        Returns whether or not the given value is considered null for this column.

        :param value: <variant>

        :return: <bool>
        """
        return type(value) is not bool and not bool(value)

    def loadJSON(self, jdata):
        """
        Initializes the information for this class from the given JSON data blob.

        :param jdata: <dict>
        """
        # required params
        self.__name = jdata['name']
        self.__field = jdata['field']

        # optional fields
        self.__display = jdata.get('display') or self.__display
        self.__flags = jdata.get('flags') or self.__flags
        self.__defaultOrder = jdata.get('defaultOrder') or self.__defaultOrder
        self.__default = jdata.get('default') or self.__default

        index = jdata.get('index')
        if index:
            self.__index = Column.Index(**index)

    def memberOf(self, schemas):
        """
        Returns a list of schemas this column is a member of from the inputted
        list.
        
        :param      schemas | [<orb.TableSchema>, ..]
        
        :return     [<orb.TableSchema>, ..]
        """
        for schema in schemas:
            if schema.hasColumn(self):
                yield schema

    def name(self):
        """
        Returns the accessor name that will be used when 
        referencing this column around the app.
        
        :return     <str>
        """
        return self.__name

    def restore(self, value, context=None, inflated=True):
        """
        Restores the value from a table cache for usage.
        
        :param      value   | <variant>
                    context | <orb.Context> || None
        """
        return value

    def store(self, value, context=None):
        """
        Converts the value to one that is safe to store on a record within
        the record values dictionary
        
        :param      value | <variant>
        
        :return     <variant>
        """
        if isinstance(value, (str, unicode)):
            value = self.valueFromString(value)
        return value

    def schema(self):
        """
        Returns the table that this column is linked to in the database.
        
        :return     <TableSchema>
        """
        return self.__schema

    def setter(self):
        return self.__setter or orb.system.syntax().setter(self.__name)

    def setDefault(self, default):
        """
        Sets the default value for this column to the inputted value.
        
        :param      default | <str>
        """
        self.__default = default

    def setDisplay(self, display):
        """
        Sets the display name for this column.
        
        :param      displayName | <str>
        """
        self.__display = display

    def setField(self, field):
        """
        Sets the field name for this column.
        
        :param      field | <str>
        """
        self.__field = field

    def setFlag(self, flag, state=True):
        """
        Sets whether or not this flag should be on.
        
        :param      flag  | <Column.Flags>
                    state | <bool>
        """
        if state:
            self.__flags |= flag
        else:
            self.__flags &= ~flag

    def setFlags(self, flags):
        """
        Sets the global flags for this column to the inputted flags.
        
        :param      flags | <Column.Flags>
        """
        self.__flags = flags

    def setName(self, name):
        """
        Sets the name of this column to the inputted name.
        
        :param      name    | <str>
        """
        self.__name = name

    def setIndex(self, index):
        """
        Sets the index instance for this column to the inputted instance.
        
        :param      index   | <orb.Column.Index> || None
        """
        self.__index = index

    def setSchema(self, schema):
        self.__schema = schema

    def testFlag(self, flag):
        """
        Tests to see if this column has the inputted flag set.
        
        :param      flag | <Column.Flags>
        """
        if isinstance(flag, (str, unicode)):
            flag = self.Flags(flag)

        return bool(self.flags() & flag) if flag >= 0 else not bool(self.flags() & ~flag)

    def validate(self, value):
        """
        Validates the inputted value against this columns rules.  If the inputted value does not pass, then
        a validation error will be raised.  Override this method in column sub-classes for more
        specialized validation.
        
        :param      value | <variant>
        
        :return     <bool> success
        """
        # check for the required flag
        if self.testFlag(self.Flags.Required) and not self.testFlag(self.Flags.AutoIncrement):
            if self.isNull(value):
                msg = '{0} is a required column.'.format(self.name())
                raise orb.errors.ColumnValidationError(self, msg)

        # otherwise, we're good
        return True

    def valueFromString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.
        
        :param      value | <str>
        """
        return projex.text.nativestring(value)

    def valueToString(self, value, context=None):
        """
        Converts the inputted string text to a value that matches the type from
        this column type.
        
        :sa         engine
        
        :param      value | <str>
                    extra | <variant>
        """
        return projex.text.nativestring(value)

    @classmethod
    def fromJSON(cls, jdata):
        """
        Generates a new column from the given json data.  This should
        be already loaded into a Python dictionary, not a JSON string.
        
        :param      jdata | <dict>
        
        :return     <orb.Column> || None
        """
        cls_type = jdata.get('type')
        col_cls = cls.byName(cls_type)

        if not col_cls:
            raise orb.errors.InvalidColumnType(cls_type)
        else:
            col = col_cls()
            col.loadJSON(jdata)
            return col


class VirtualColumn(Column):
    def __init__(self, **kwds):
        super(VirtualColumn, self).__init__(**kwds)

        # set default properties
        self.setFlag(Column.Flags.Virtual, True)
