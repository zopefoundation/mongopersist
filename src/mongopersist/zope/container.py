##############################################################################
#
# Copyright (c) 2011 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Mongo Persistence Zope Containers"""
import UserDict
import persistent
import pymongo.dbref
import zope.component
from rwproperty import getproperty, setproperty
from zope.container import contained, sample
from zope.container.interfaces import IContainer

from mongopersist import interfaces, serialize
from mongopersist.zope import interfaces as zinterfaces

class MongoContained(contained.Contained):

    @getproperty
    def __name__(self):
        return getattr(self, '_v_key', None)
    @setproperty
    def __name__(self, value):
        setattr(self, '_v_key', value)

    @getproperty
    def __parent__(self):
        return getattr(self, '_v_parent', None)
    @setproperty
    def __parent__(self, value):
        setattr(self, '_v_parent', value)


class SimpleMongoContainer(sample.SampleContainer, persistent.Persistent):
    _m_remove_documents = True

    def __getstate__(self):
        state = super(SimpleMongoContainer, self).__getstate__()
        state['data'] = state.pop('_SampleContainer__data')
        return state

    def __setstate__(self, state):
        # Mongopersist always reads a dictionary as persistent dictionary. And
        # modifying this dictionary will cause the persistence mechanism to
        # kick in. So we create a new object that we can easily modify without
        # harm.
        state = dict(state)
        state['_SampleContainer__data'] = state.pop('data', {})
        super(SimpleMongoContainer, self).__setstate__(state)

    def __getitem__(self, key):
        obj = super(SimpleMongoContainer, self).__getitem__(key)
        obj._v_key = key
        obj._v_parent = self
        return obj

    def get(self, key, default=None):
        '''See interface `IReadContainer`'''
        obj = super(SimpleMongoContainer, self).get(key, default)
        if obj is not default:
            obj._v_key = key
            obj._v_parent = self
        return obj

    def items(self):
        items = super(SimpleMongoContainer, self).items()
        for key, obj in items:
            obj._v_key = key
            obj._v_parent = self
        return items

    def values(self):
        return [v for k, v in self.items()]

    def __setitem__(self, key, obj):
        super(SimpleMongoContainer, self).__setitem__(key, obj)
        self._p_changed = True

    def __delitem__(self, key):
        obj = self[key]
        super(SimpleMongoContainer, self).__delitem__(key)
        if self._m_remove_documents:
            self._p_jar.remove(obj)
        self._p_changed = True


class MongoContainer(contained.Contained,
                     persistent.Persistent,
                     UserDict.DictMixin):
    zope.interface.implements(IContainer, zinterfaces.IMongoContainer)
    _m_database = None
    _m_collection = None
    _m_mapping_key = 'key'
    _m_parent_key = 'parent'
    _m_remove_documents = True

    def __init__(self, collection=None, database=None,
                 mapping_key=None, parent_key=None):
        if collection:
            self._m_collection = collection
        if database:
            self._m_database = database
        if mapping_key is not None:
            self._m_mapping_key = mapping_key
        if parent_key is not None:
            self._m_parent_key = parent_key

    @property
    def _m_jar(self):
        # If the container is in a Mongo storage hierarchy, then getting the
        # datamanager is easy, otherwise we do an adapter lookup.
        if interfaces.IMongoDataManager.providedBy(self._p_jar):
            return self._p_jar
        else:
            provider = zope.component.getUtility(
                interfaces.IMongoDataManagerProvider)
            return provider.get()

    def get_collection(self):
        db_name = self._m_database or self._m_jar.default_database
        return self._m_jar.get_collection(db_name, self._m_collection)

    def _m_get_parent_key_value(self):
        if getattr(self, '_p_jar', None) is None:
            raise ValueError('_p_jar not found.')
        if interfaces.IMongoDataManager.providedBy(self._p_jar):
            return self
        else:
            return 'zodb-'+''.join("%02x" % ord(x) for x in self._p_oid).strip()

    def _m_get_items_filter(self):
        filter = {}
        # Make sure that we only look through objects that have the mapping
        # key. Objects not having the mapping key cannot be part of the
        # collection.
        if self._m_mapping_key is not None:
            filter[self._m_mapping_key] = {'$exists': True}
        if self._m_parent_key is not None:
            gs = self._m_jar._writer.get_state
            filter[self._m_parent_key] = gs(self._m_get_parent_key_value())
        return filter

    def _load_one(self, doc):
        # Create a DBRef object and then load the full state of the object.
        dbref = pymongo.dbref.DBRef(
            self._m_collection, doc['_id'],
            self._m_database or self._m_jar.default_database)
        obj = self._m_jar._reader.get_ghost(dbref)
        self._m_jar.setstate(obj, doc)
        obj._v_key = doc[self._m_mapping_key]
        obj._v_parent = self
        return obj

    def __getitem__(self, key):
        filter = self._m_get_items_filter()
        filter[self._m_mapping_key] = key
        coll = self.get_collection()
        doc = coll.find_one(filter)
        if doc is None:
            raise KeyError(key)
        return self._load_one(doc)

    def _real_setitem(self, key, value):
        # This call by iteself caues the state to change _p_changed to True.
        setattr(value, self._m_mapping_key, key)
        if self._m_parent_key is not None:
            setattr(value, self._m_parent_key, self._m_get_parent_key_value())

    def __setitem__(self, key, value):
        # Make sure the value is in the database, since we might want to use
        # its oid.
        if value._p_oid is None:
            self._m_jar.insert(value)
        # When the key is None, we use the object is as name.
        if key is None:
            key = unicode(value._p_oid.id)
        # We want to be as close as possible to using the Zope semantics.
        contained.setitem(self, self._real_setitem, key, value)

    def add(self, value, key=None):
        # We are already suporting ``None`` valued keys, which prompts the key
        # to be the OID. But people felt that a more explicit interface would
        # be better in this case.
        self[key] = value

    def __delitem__(self, key):
        value = self[key]
        # First remove the parent and name from the object.
        if self._m_mapping_key is not None:
            delattr(value, self._m_mapping_key)
        if self._m_parent_key is not None:
            delattr(value, self._m_parent_key)
        # Let's now remove the object from the database.
        if self._m_remove_documents:
            self._m_jar.remove(value)
        # Send the uncontained event.
        contained.uncontained(value, self, key)

    def keys(self):
        filter = self._m_get_items_filter()
        filter[self._m_mapping_key] = {'$ne': None}
        coll = self.get_collection()
        return [doc[self._m_mapping_key]
                for doc in coll.find(filter, fields=(self._m_mapping_key,))]

    def raw_find(self, spec=None, *args, **kwargs):
        if spec is None:
            spec  = {}
        spec.update(self._m_get_items_filter())
        coll = self.get_collection()
        return coll.find(spec, *args, **kwargs)

    def find(self, spec=None, *args, **kwargs):
        # Search for matching objects.
        result = self.raw_find(spec, *args, **kwargs)
        for doc in result:
            obj = self._load_one(doc)
            yield obj

    def raw_find_one(self, spec_or_id=None, *args, **kwargs):
        if spec_or_id is None:
            spec_or_id  = {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': spec_or_id}
        spec_or_id.update(self._m_get_items_filter())
        coll = self.get_collection()
        return coll.find_one(spec_or_id, *args, **kwargs)

    def find_one(self, spec_or_id=None, *args, **kwargs):
        doc = self.raw_find_one(spec_or_id, *args, **kwargs)
        if doc is None:
            return None
        return self._load_one(doc)

class AllItemsMongoContainer(MongoContainer):
    _m_parent_key = None


class SubDocumentMongoContainer(MongoContained, MongoContainer):
    _p_mongo_sub_object = True
