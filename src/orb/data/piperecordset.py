#!/usr/bin/python

""" Defines an piping system to use when accessing multi-to-multi records. """

# define authorship information
__authors__         = ['Eric Hulser']
__author__          = ','.join(__authors__)
__credits__         = []
__copyright__       = 'Copyright (c) 2011, Projex Software'
__license__         = 'LGPL'

# maintanence information
__maintainer__      = 'Projex Software'
__email__           = 'team@projexsoftware.com'

#------------------------------------------------------------------------------

from projex.lazymodule import LazyModule
from projex.text import nativestring as nstr
from xml.etree import ElementTree

from .recordset import RecordSet

orb = LazyModule('orb')


class PipeRecordSet(RecordSet):
    def __init__(self, 
                 records,
                 source,
                 pipeTable=None,
                 sourceColumn='',
                 targetColumn=''):
        
        super(PipeRecordSet, self).__init__(records)
        
        # define additional properties
        self._source = source
        self._pipeTable = pipeTable
        self._sourceColumn = sourceColumn
        self._targetColumn = targetColumn
    
    def addRecord(self, record, **options):
        """
        Adds a new record for this pipe set.  The inputed record should refer
        to the value for the target column and be an instance of the
        target type.
        
        :param      record | <orb.Table>
        
        :return     <orb.Table> || None | instance of the pipe table
        """
        # make sure we have a valid table and record
        table = self.table()
        if not (table and record and record.isRecord() \
                and isinstance(record, table)):
            return None
        
        # make sure we have a pipe table
        if not self._pipeTable:
            return None
        
        pipe = self._pipeTable
        unique = options.pop('uniqueRecord', True)
        
        if unique:
            q  = orb.Query(pipe, self._sourceColumn) == self._source
            q &= orb.Query(pipe, self._targetColumn) == record
            
            if pipe.selectFirst(where = q):
                return None
        
        options[self._sourceColumn] = self._source
        options[self._targetColumn] = record
        
        link = pipe(**options)
        link.commit()
        
        return link
    
    def clear(self, **options):
        """
        Clears all the records through the pipeset based on the inputed
        parameters.
        
        :return     <int>
        """
        pipe = self._pipeTable
        if not pipe:
            return 0
        
        q  = orb.Query(pipe, self._sourceColumn) == self._source
        
        where = options.pop('where', None)
        q &= where
        
        for key, value in options.items():
            q &= orb.Query(pipe, key) == value
        
        return pipe.select(where = q).remove()
    
    def hasRecord(self, record):
        """
        Checks to see if the given record is in the record set for this
        instance.
        
        :param      record | <orb.Table>
        
        :return     <bool>
        """
        table = self.table()
        if not (table and record and record.isRecord() \
                and isinstance(record, table)):
            return False
        
        where = self.query() & (orb.Query(table) == record)
        return table.selectFirst(where = where) != None
    
    def removeRecord(self, record, **options):
        """
        Removes the record from this record set and from the database based
        on the piping information.
        
        :param      record | <orb.Table>
        
        :return     <int> | number of records removed
        """
        table = self.table()
        if not (table and record and record.isRecord() \
                and isinstance(record, table)):
            return 0
        
        pipe = self._pipeTable
        if not pipe:
            return 0
        
        unique = options.pop('uniqueRecord', True)
        
        q  = orb.Query(pipe, self._sourceColumn) == self._source
        q &= orb.Query(pipe, self._targetColumn) == record
        
        for key, value in options.items():
            q &= orb.Query(pipe, key) == value
        
        return pipe.select(where = q).remove()
    
    def remove(self, **options):
        """
        Removes all the links within this set.  This will remove
        from the Pipe table and not from the source record.
        """
        pipe = self._pipeTable
        if not pipe:
            return 0
        
        q  = orb.Query(pipe, self._sourceColumn) == self._source
        
        for key, value in options.items():
            q &= orb.Query(pipe, key) == value
        
        return pipe.select(where = q).remove()

