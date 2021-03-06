import xml.etree.ElementTree as ET
from zope import component
from zope import interface
from zope.component.factory import Factory
from zope.schema.interfaces import ICollection
from zope.schema.interfaces import IBool
from zope.schema.interfaces import IDate, IDatetime
from zope.schema.interfaces import IDecimal, IFloat, IInt
from zope.schema.interfaces import IDict
from zope.schema.interfaces import IDottedName
from zope.schema.interfaces import IText, INativeString
from zope.schema.fieldproperty import FieldProperty
from sparc.db.splunk import xml_ns
from sparc.utils.requests import IRequest
from interfaces import ISPlunkKVCollectionIdentifier
from interfaces import ISplunkKVCollectionSchema
from interfaces import ISplunkConnectionInfo

sm = component.getSiteManager()
def current_kv_names(sci, app_user, app_name, request=None):
    """Return set of string names of current available Splunk KV collections
    
    Args:
        sci: Instance of sparc.db.splunk.ISplunkConnectionInfo
        app_user: Splunk KV Collection app user to reference
        app_name: Splunk KV Collection application name to reference
    kwargs:
        request: instance of sparc.utils.requests.IRequest
        
    request will be resolved via 'sparc.utils.requests.request_resolver' named
    component.

    Returns:
        Set of string names for collections found
    """
    resolver = sm.getUtility(IRequest, u'sparc.utils.requests.request_resolver')
    req = resolver(request=request)
    kwargs = {'auth': (sci['username'], sci['password'], )}
    url = "".join(['https://',sci['host'],':',sci['port'],
                                    '/servicesNS/',app_user,'/',
                                                        app_name,'/'])
    _return = set()
    r = req.request('get', url+"storage/collections/config", **kwargs)
    r.raise_for_status()
    root = ET.fromstring(r.text)
    for entry in root.findall('./atom:entry', xml_ns):
        name = entry.find('./atom:title', xml_ns).text
        if not name:
            raise ValueError('unexpectedly found empty collection title')
        _return.add(name)
    return _return

@interface.implementer(ISPlunkKVCollectionIdentifier)
class SplunkKVCollectionIdentifier(object):
    
    def __init__(self, **kwargs):
        for k in kwargs:
            setattr(self, k, kwargs[k])
    collection = FieldProperty(ISPlunkKVCollectionIdentifier['collection'])
    application = FieldProperty(ISPlunkKVCollectionIdentifier['application'])
    username = FieldProperty(ISPlunkKVCollectionIdentifier['username'])
splunkKVCollectionIdentifierFactory = Factory(SplunkKVCollectionIdentifier)

@interface.implementer(ISplunkKVCollectionSchema)
class SplunkKVCollectionSchema(dict):
    pass
splunkKVCollectionSchemaFactory = Factory(SplunkKVCollectionSchema)

@interface.implementer(ISplunkKVCollectionSchema)
@component.adapter(ISplunkConnectionInfo, ISPlunkKVCollectionIdentifier, IRequest)
class SplunkKVCollectionSchemaFromSplunkInstance(dict):
    
    def __init__(self, sci, kv_id, request):
        self.sci = sci
        self.kv_id = kv_id
        self.req = request
        self.collname = kv_id.collection
        self.appname = kv_id.application
        self.username = kv_id.username
        self.url = "".join(['https://',sci['host'],':',sci['port'],
                                    '/servicesNS/',self.username,'/',
                                                        self.appname,'/'])
        self.auth = (self.sci['username'], self.sci['password'], )
        
        r = self.req.request('get',
                        self.url+"storage/collections/config/"+self.collname,
                        data={'output_mode': 'json'},
                        auth=self.auth)
        r.raise_for_status()
        
        
        data = r.json()
        if 'entry' in data:
            for entry in data['entry']:
                if 'name' in entry and not entry['name'] == self.collname:
                    continue
                if 'content' in entry:
                    for k in [k for k in entry['content'] if k.startswith('field.')]:
                        self[k] = entry['content'][k]

# In the future, we may want to improve the usability of this adapter by
# providing Splunk KV specific markers to help identify override default field
# conversion behavior.
@interface.implementer(ISplunkKVCollectionSchema)
@component.adapter(interface.Interface)
class SplunkKVCollectionSchemaFromZopeSchema(dict):
    
    def __init__(self, context):
        self.context = context
        super(SplunkKVCollectionSchemaFromZopeSchema, self).__init__()
        schema = self.get_collection_schema_from_interface_schema(context)
        for name in schema:
            self[name] = schema[name]

    @classmethod
    def get_collection_schema_from_interface_schema(self, schema):
        collection = {}
        for name in schema:
            if IDate.providedBy(schema[name]) or \
                                                IDatetime.providedBy(schema[name]):
                collection['field.'+name] = 'time'
            elif IDecimal.providedBy(schema[name]) or \
                                IFloat.providedBy(schema[name]) or \
                                               IInt.providedBy(schema[name]):
                collection['field.'+name] = 'number'
            elif IBool.providedBy(schema[name]):
                collection['field.'+name] = 'bool'
            elif ICollection.providedBy(schema[name]):
                if not ICollection.providedBy(schema[name].value_type) and not \
                            IDict.providedBy(schema[name].value_type):
                    collection['field.'+name] = 'array'
            elif IDict.providedBy(schema[name]):
                if IText.providedBy(schema[name].key_type) and \
                            IText.providedBy(schema[name].value_type):
                    collection['field.'+name] = 'array'
            # this is a pretty weak check for a IP address field.  We might want
            # to update this to look for a field validator based on the ipaddress package
            # or mark this field with a special interface indicating it is an 
            # IP address
            elif IDottedName.providedBy(schema[name]) and \
                            (schema[name].min_dots == schema[name].max_dots == 3):
                collection['field.'+name] = 'cidr'
            elif IText.providedBy(schema[name]) or \
                                        INativeString.providedBy(schema[name]):
                collection['field.'+name] = 'string'
        return collection