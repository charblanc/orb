#!/usr/bin/python

""" 
Defines the main Table class that will be used when developing
database classes.
"""

# define authorship information
__authors__ = ['Eric Hulser']
__author__ = ','.join(__authors__)
__credits__ = []
__copyright__ = 'Copyright (c) 2011, Projex Software'
__license__ = 'LGPL'

# maintanence information
__maintainer__ = 'Projex Software'
__email__ = 'team@projexsoftware.com'

# ------------------------------------------------------------------------------

import orb

from new import instancemethod

TEMP_REVERSE_LOOKUPS = {}
GETTER_DOCS = """\
Gets the value for the {name} column.
{optdocs}

This method was auto-generated from ORB.

:param      {optparams}

:return     {returns}
"""

GETTER_I18N_DOCS = """\

:internationalization   You can provide the optional locale parameter to get
                        the locale-specific value for this column.  If no
                        locale is provided, the current global locale
                        defined in the [[Orb]] instance will be used.
"""

GETTER_FKEY_DOCS = """\

:references             Foreign key references will be inflated to their
                        API class type.  If you want simply the key value
                        from the database instead of a record, then you
                        can pass inflated = False.  This
                        can improve overhead when working with large amounts
                        of records at one time.
"""

# ----------------------------------------------------------------------

SETTER_DOCS = """\
Sets the value for the {name} column to the inputed value.  This will only
affect the value of the API object instance, and '''not''' the value in
the database.  To commit this value to the database, use the [[Table::commit]]
method.  This method will return a boolean whether or not a change in the
value acutally occurred.

:param      {param}  | {returns}
{optparams}

:return     <bool> | changed
"""

SETTER_I18N_DOCS = """\
:internationalization   If this column is translatable, you can provide the
                        optional locale parameter to set the locale-specific
                        value for this column.  If no locale is provided, the
                        current global locale defined in the [[Orb]] instance
                        will be used.
"""


#------------------------------------------------------------------------------

class gettermethod(object):
    """ Creates a method for tables to use as a field accessor. """

    def __init__(self, **kwds):
        """
        Defines the getter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the MetaTable class when
        generating column methods on a model.
        """
        self.__dict__.update(kwds)
        self.columnName = kwds.get('columnName', '')

        translatable = kwds.get('translatable', False)
        inflatable = kwds.get('inflatable', False)

        args = []
        optdocs = []
        optparams = []

        if translatable:
            args.append('locale=None')
            optdocs.append(GETTER_I18N_DOCS)
            optparams.append('            locale    | None || <str> locale code')

        if inflatable:
            args.append('inflated=True')
            optdocs.append(GETTER_FKEY_DOCS)
            optparams.append('            inflated    | <bool>')

        default = '<variant>  (see {0} for details)'.format(self.columnName)
        returns = kwds.get('returns', default)

        self.func_name = kwds['__name__']
        self.func_args = '({0})'.format(', '.join(args))
        self.func_doc = GETTER_DOCS.format(name=self.columnName,
                                           optdocs='\n'.join(optdocs),
                                           optparams='\n'.join(optparams),
                                           returns=returns)

        self.__dict__['__doc__'] = self.func_doc

    def __call__(self, record, **kwds):
        """
        Calls the getter lookup method for the database record.
        
        :param      record      <Table>
        """
        inflated = kwds.get('inflated', True)
        val = record.recordValue(self.columnName,
                                 locale=kwds.get('locale', None),
                                 default=kwds.get('default', None),
                                 inflated=inflated,
                                 useMethod=False)

        if not inflated and orb.Table.recordcheck(val):
            return val.primaryKey()
        return val


#------------------------------------------------------------------------------

class settermethod(object):
    """ Defines a method for setting database fields on a Table instance. """

    def __init__(self, **kwds):
        """
        Defines the setter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the MetaTable class when
        generating column methods on a model
        """
        self.__dict__.update(kwds)
        self.columnName = kwds.get('columnName', '')

        if kwds.get('inflatable'):
            args = ['record_or_key']
        else:
            args = ['value']
        optdocs = []
        optparams = []

        if kwds.get('translatable'):
            args.append('locale=None')
            optdocs.append(SETTER_I18N_DOCS)
            optparams.append('            locale    | None || <str>')

        default = '<variant>  (see {0} for details)'.format(self.columnName)
        returns = kwds.get('returns', default)

        self.func_name = kwds['__name__']
        self.func_args = '({0})'.format(', '.join(args))
        self.func_doc = SETTER_DOCS.format(name=self.columnName,
                                           param=args[0],
                                           optdocs='\n'.join(optdocs),
                                           optparams='\n'.join(optparams),
                                           returns=returns)

        self.__dict__['__doc__'] = self.func_doc

    def __call__(self, record, value, **kwds):
        """
        Calls the setter method for the inputed database record.
        
        :param      record      <Table>
                    value       <variant>
        """
        kwds['useMethod'] = False
        return record.setRecordValue(self.columnName, value, **kwds)


#----------------------------------------------------------------------

class reverselookupmethod(object):
    """ Defines a reverse lookup method for lookup up relations. """

    def __init__(self, **kwds):
        """
        Defines the getter method that will be used when accessing
        information about a column on a database record.  This
        class should only be used by the MetaTable class when
        generating column methods on a model.
        """
        self.__dict__.update(kwds)
        self.__lookup__ = True

        self._cache = {}
        self._local_cache = {}
        self.reference = kwds.get('reference', '')
        self.referenceDb = kwds.get('referenceDatabase', None)
        self.columnName = kwds.get('columnName', '')
        self.unique = kwds.get('unique', False)
        self.cached = kwds.get('cached', False)
        self.cacheTimeout = kwds.get('cacheTimeout', 0)
        self.func_name = kwds['__name__']
        self.func_args = '()'
        self.func_doc = 'Auto-generated Orb reverse lookup method'

        self.__dict__['__doc__'] = self.func_doc

    def __call__(self, record, **kwds):
        """
        Calls the getter lookup method for the database record.
        
        :param      record      <Table>
        """
        reload = kwds.pop('reload', False)

        # remove any invalid query lookups
        if 'where' in kwds and orb.Query.testNull(kwds['where']):
            kwds.pop('where')

        # lookup the records with a specific model
        kwds.setdefault('locale', record.recordLocale())
        table = kwds.get('table') or self.tableFor(record)
        if not table:
            return None if self.unique else orb.RecordSet()

        # return from the cache when specified
        cache = self.cache(table)
        cache_key = (record.id(),
                     hash(orb.LookupOptions(**kwds)),
                     record.database().name())

        if not reload and cache and cache_key in self._local_cache and cache.isCached(cache_key):
            out = self._local_cache[cache_key]
            out.updateOptions(**kwds)
            return out

        self._local_cache.pop(cache_key, None)

        # make sure this is a valid record
        if not record.isRecord():
            if self.unique:
                return None
            return orb.RecordSet()

        # generate the reverse lookup query
        reverse_q = orb.Query(self.columnName) == record

        kwds['where'] = reverse_q & kwds.get('where')
        kwds['db'] = record.database()
        kwds['context'] = record.schema().context(self.func_name)

        if self.unique:
            output = table.selectFirst(**kwds)
        else:
            output = table.select(**kwds)

        if isinstance(output, orb.RecordSet):
            output.setSource(record)
            output.setSourceColumn(self.columnName)

        if cache and output is not None:
            self._locale_cache[cache_key] = output
            cache.setValue(cache_key, True, timeout=self.cacheTimeout)

        return output

    def cache(self, table, force=False):
        """
        Returns the cache for this table.
        
        :param      table | <subclass of orb.Table>
        
        :return     <orb.TableCache> || None
        """
        try:
            return self._cache[table]
        except KeyError:
            if force or self.cached:
                cache = table.tableCache() or orb.TableCache(table, table.schema().cache(), timeout=self.cacheTimeout)
                self._cache[table] = cache
                return cache
            return None

    def preload(self, record, data, lookup, type='records'):
        """
        Preload a list of records from the database.

        :param      record  | <orb.Table>
                    data    | [<dict>, ..]
                    lookup | <orb.LookupOptions> || None
        """
        table = self.tableFor(record)
        cache_key = (record.id(),
                     hash(lookup or orb.LookupOptions()),
                     record.database().name())

        cache = self.cache(table, force=True)
        rset = self._local_cache.get(cache_key)
        if rset is None:
            rset = orb.RecordSet()
            self._local_cache[cache_key] = rset
            cache.setValue(cache_key, True, timeout=self.cacheTimeout)

        if type == 'ids':
            rset.cache('ids', data)
        elif type == 'count':
            rset.cache('count', data)
        elif type == 'first':
            rset.cache('first', table(__values=data) if data else None)
        elif type == 'last':
            rset.cache('last', table(__values=data) if data else None)
        else:
            rset.cache('records', [table(__values=record) for record in data or []])

    def tableFor(self, record):
        """
        Returns the table for the inputed record.

        :return     <orb.Table>
        """
        return record.polymorphicModel(self.reference) or \
                orb.system.model(self.reference, database=self.referenceDb)

#------------------------------------------------------------------------------

class MetaTable(type):
    """ 
    Defines the table Meta class that will be used to dynamically generate
    Table class types.
    """

    def __new__(mcs, name, bases, attrs):
        """
        Manages the creation of database model classes, reading
        through the creation attributes and generating table
        schemas based on the inputed information.  This class
        never needs to be expressly defined, as any class that
        inherits from the Table class will be passed through this
        as a constructor.
        
        :param      mcs         <MetaTable>
        :param      name        <str>
        :param      bases       <tuple> (<object> base,)
        :param      attrs       <dict> properties
        
        :return     <type>
        """
        # ignore initial class
        db_ignore = attrs.pop('__db_ignore__', False)
        if db_ignore:
            return super(MetaTable, mcs).__new__(mcs, name, bases, attrs)

        base_tables = [base for base in bases if isinstance(base, MetaTable)]
        base_data = {key: value for base_table in base_tables
                                for key, value in base_table.__dict__.items() if key.startswith('__db_')}

        # define the default database information
        db_data = {
            '__db__': None,
            '__db_group__': 'Default',
            '__db_name__': name,
            '__db_dbname__': '',
            '__db_columns__': [],
            '__db_indexes__': [],
            '__db_pipes__': [],
            '__db_contexts__': {},
            '__db_views__': {},
            '__db_schema__': None,
            '__db_inherits__': None,
            '__db_abstract__': False,
            '__db_archived__': False
        }
        # override with any inherited data
        db_data.update(base_data)

        # override with any custom data
        db_data.update({key: value for key, value in attrs.items() if key.startswith('__db_')})

        if not db_data['__db_inherits__'] and base_tables and base_tables[0].schema():
            db_data['__db_inherits__'] = base_tables[0].schema().name()

        # create a new model for this table
        return mcs.createModel(mcs, name, bases, attrs, db_data)

    @staticmethod
    def createModel(mcs, name, bases, attrs, db_data):
        """
        Create a new table model.
        """
        new_model = super(MetaTable, mcs).__new__(mcs, name, bases, attrs)
        schema = db_data['__db_schema__']

        if schema:
            db_data['__db_name__'] = schema.name()
            db_data['__db_dbname__'] = schema.dbname()

            try:
                schema.setAutoLocalize(db_data['__db_autolocalize__'])
            except KeyError:
                pass
            try:
                schema.setAutoPrimary(db_data['__db_autoprimary__'])
            except KeyError:
                pass

            if db_data['__db_columns__']:
                columns = schema.columns(recurse=False) + db_data['__db_columns__']
                schema.setColumns(columns)
            if db_data['__db_indexes__']:
                indexes = schema.indexes() + db_data['__db_indexes__']
                schema.setIndexes(indexes)
            if db_data['__db_pipes__']:
                pipes = schema.pipes() + db_data['__db_pipes__']
                schema.setPipes(pipes)
            if db_data['__db_contexts__']:
                contexts = dict(schema.contexts())
                contexts.update(db_data['__db_contexts__'])
                schema.setContexts(contexts)
            if db_data['__db_views__']:
                for name, view in db_data['__db_views__'].items():
                    schema.setView(name, view)
        else:
            # create the table schema
            schema = orb.TableSchema()
            schema.setDatabase(db_data['__db__'])
            schema.setAutoPrimary(db_data.get('__db_autoprimary__', True))
            schema.setAutoLocalize(db_data.get('__db_autolocalize__', False))
            schema.setName(db_data['__db_name__'] or name)
            schema.setGroupName(db_data['__db_group__'])
            schema.setDbName(db_data['__db_dbname__'])
            schema.setAbstract(db_data['__db_abstract__'])
            schema.setColumns(db_data['__db_columns__'])
            schema.setIndexes(db_data['__db_indexes__'])
            schema.setPipes(db_data['__db_pipes__'])
            schema.setInherits(db_data['__db_inherits__'])
            schema.setArchived(db_data['__db_archived__'])
            schema.setContexts(db_data['__db_contexts__'])

            for name, view in db_data['__db_views__'].items():
                schema.setView(name, view)

            schema.setModel(new_model)

            orb.system.registerSchema(schema)

        db_data['__db_schema__'] = schema

        # add the db values to the class
        for key, value in db_data.items():
            setattr(new_model, key, value)

        # create class methods for the index instances
        for index in schema.indexes():
            iname = index.name()
            if not hasattr(new_model, iname):
                setattr(new_model, index.name(), classmethod(index))

        # create instance methods for the pipe instances
        for pipe in schema.pipes():
            pname = pipe.name()
            if not hasattr(new_model, pname):
                pipemethod = instancemethod(pipe, None, new_model)
                setattr(new_model, pname, pipemethod)

        # pylint: disable-msg=W0212
        columns = schema.columns(recurse=False)
        for column in columns:
            colname = column.name()

            # create getter method
            gname = column.getterName()
            if gname and not hasattr(new_model, gname):
                gmethod = gettermethod(columnName=colname,
                                       translatable=column.isTranslatable(),
                                       inflatable=column.isReference(),
                                       returns=column.returnType(),
                                       __name__=gname)

                getter = instancemethod(gmethod, None, new_model)
                setattr(new_model, gname, getter)

            # create setter method
            sname = column.setterName()
            if sname and not (column.isReadOnly() or hasattr(new_model, sname)):
                smethod = settermethod(columnName=colname,
                                       translatable=column.isTranslatable(),
                                       inflatable=column.isReference(),
                                       returns=column.returnType(),
                                       __name__=sname)
                setter = instancemethod(smethod, None, new_model)
                setattr(new_model, sname, setter)

            # create an index if necessary
            iname = column.indexName()
            if column.indexed() and iname and not hasattr(new_model, iname):
                index = orb.Index(iname,
                                  [column.name()],
                                  unique=column.unique())
                index.setCached(column.indexCached())
                index.setCacheTimeout(column.indexCacheTimeout())
                index.__name__ = iname
                imethod = classmethod(index)
                setattr(new_model, iname, imethod)

            # create a reverse lookup
            if column.isReversed() and column.schema().name() == db_data['__db_name__']:
                rev_name = column.reversedName()
                rev_cached = column.reversedCached()
                ref_name = column.reference()

                try:
                    ref_model = column.referenceModel()
                except orb.errors.TableNotFound:
                    ref_model = None

                rev_cacheTimeout = column.reversedCacheTimeout()

                # create the lookup method
                lookup = reverselookupmethod(columnName=column.name(),
                                             reference=db_data['__db_name__'],
                                             unique=column.unique(),
                                             cached=rev_cached,
                                             cacheTimeout=rev_cacheTimeout,
                                             __name__=rev_name)

                # ensure we're assigning it to the proper base module
                while ref_model and \
                      ref_model.__module__ != 'orb.schema.dynamic' and \
                      ref_model.__bases__ and \
                      ref_model.__bases__[0] == orb.Table:
                    ref_model = ref_model.__bases__[0]

                # assign to an existing model
                if ref_model:
                    ilookup = instancemethod(lookup, None, ref_model)
                    setattr(ref_model, rev_name, ilookup)
                else:
                    TEMP_REVERSE_LOOKUPS.setdefault(ref_name, [])
                    TEMP_REVERSE_LOOKUPS[ref_name].append((rev_name, lookup))

        # assign any cached reverse lookups to this model
        lookups = TEMP_REVERSE_LOOKUPS.pop(db_data['__db_name__'], [])
        for rev_name, lookup in lookups:
            ilookup = instancemethod(lookup, None, new_model)
            setattr(new_model, rev_name, ilookup)

        return new_model