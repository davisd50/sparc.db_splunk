from splunklib.results import ResultsReader
from zope import component
from zope import interface
from zope.component.factory import Factory
from sparc.db import IQueryResultSet
from sparc.db import ITabularResult
from sparc.db.query import DbQuery
from sparc.db.splunk import ISplunkQuery
from sparc.db.splunk import ISplunkResultsStream

@interface.implementer()
class SplunkQuery(DbQuery):
    pass

splunkQueryFactory = Factory(SplunkQuery)

@interface.implementer(IQueryResultSet)
@component.adapter(ISplunkResultsStream)
class QueryResultSetForSplunk(object):
    """A database query with results"""
    
    def __init__(self, context):
        self.context = context

    def __iter__(self):
        """Iterator of IResult objects"""
        _seq = []
        for ordered_dict in ResultsReader(self.context):
            for key, value in ordered_dict.iteritems():
                if isinstance(value, basestring):
                    ordered_dict[key] = component.createObject(u'sparc.db.result_value', value)
                else:
                    ordered_dict[key] = component.createObject(u'sparc.db.result_multi_value', value)
            interface.alsoProvides(ordered_dict, ITabularResult)
            _seq.append(ordered_dict)
        return iter(_seq)

