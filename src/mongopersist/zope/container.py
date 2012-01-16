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

    def __getstate__(self):
        state = super(SimpleMongoContainer, self).__getstate__()
        state['data'] = state.pop('_SampleContainer__data')
        return state

    def __setstate__(self, state):
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

    def __setitem__(self, key, object):
        super(SimpleMongoContainer, self).__setitem__(key, object)
        self._p_changed = True

    def __delitem__(self, key):
        super(SimpleMongoContainer, self).__delitem__(key)
        self._p_changed = True


class MongoContainer(contained.Contained,
                     persistent.Persistent,
                     UserDict.DictMixin):
    zope.interface.implements(IContainer, zinterfaces.IMongoContainer)
    _m_database = None
    _m_collection = None
    _m_mapping_key = 'key'
    _m_parent_key = 'parent'

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
    def _added(self):
        ann = self._m_jar.annotations.setdefault(self._p_oid or id(self), {})
        return ann.setdefault('added', {})

    @property
    def _deleted(self):
        ann = self._m_jar.annotations.setdefault(self._p_oid or id(self), {})
        return ann.setdefault('deleted', {})

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
        return self._m_jar._conn[db_name][self._m_collection]

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

    def __getitem__(self, key):
        if key in self._added:
            return self._added[key]
        if key in self._deleted:
            raise KeyError(key)
        filter = self._m_get_items_filter()
        filter[self._m_mapping_key] = key
        doc = self.get_collection().find_one(filter, fields=())
        if doc is None:
            raise KeyError(key)
        dbref = pymongo.dbref.DBRef(
            self._m_collection, doc['_id'],
            self._m_database or self._m_jar.default_database)
        obj = self._m_jar._reader.get_ghost(dbref)
        obj._v_key = key
        obj._v_parent = self
        return obj


    def _real_setitem(self, key, value):
        # This call by iteself caues the state to change _p_changed to True.
        setattr(value, self._m_mapping_key, key)
        if self._m_parent_key is not None:
            setattr(value, self._m_parent_key, self._m_get_parent_key_value())
        self._m_jar.register(value)
        # Temporarily store the added object, so it is immediately available
        # via the API.
        self._added[key] = value
        self._deleted.pop(key, None)

    def __setitem__(self, key, value):
        contained.setitem(self, self._real_setitem, key, value)

    def __delitem__(self, key):
        # Deleting the object from the database is not our job. We simply
        # remove it from the dictionary.
        value = self[key]
        if self._m_mapping_key is not None:
            delattr(value, self._m_mapping_key)
        if self._m_parent_key is not None:
            delattr(value, self._m_parent_key)
        self._deleted[key] = value
        self._added.pop(key, None)
        contained.uncontained(value, self, key)

    def keys(self):
        filter = self._m_get_items_filter()
        filter[self._m_mapping_key] = {'$ne': None}
        keys = [
            doc[self._m_mapping_key]
            for doc in self.get_collection().find(filter)
            if not doc[self._m_mapping_key] in self._deleted]
        keys += self._added.keys()
        return keys

    def raw_find(self, spec=None, *args, **kwargs):
        if spec is None:
            spec  = {}
        spec.update(self._m_get_items_filter())
        return self.get_collection().find(spec, *args, **kwargs)

    def find(self, spec=None, fields=None, *args, **kwargs):
        # If fields were not specified, we only request the oid and the key.
        fields = tuple(fields or ())
        fields += (self._m_mapping_key,)
        result = self.raw_find(spec, fields, *args, **kwargs)
        for doc in result:
            dbref = pymongo.dbref.DBRef(
                self._m_collection, doc['_id'],
                self._m_database or self._m_jar.default_database)
            obj = self._m_jar._reader.get_ghost(dbref)
            obj._v_key = doc[self._m_mapping_key]
            obj._v_parent = self
            yield obj

    def raw_find_one(self, spec_or_id=None, *args, **kwargs):
        if spec_or_id is None:
            spec_or_id  = {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': spec_or_id}
        spec_or_id.update(self._m_get_items_filter())
        return self.get_collection().find_one(spec_or_id, *args, **kwargs)

    def find_one(self, spec_or_id=None, fields=None, *args, **kwargs):
        # If fields were not specified, we only request the oid and the key.
        fields = tuple(fields or ())
        fields += (self._m_mapping_key,)
        doc = self.raw_find_one(spec_or_id, fields, *args, **kwargs)
        if doc is None:
            return None
        dbref = pymongo.dbref.DBRef(
            self._m_collection, doc['_id'],
            self._m_database or self._m_jar.default_database)
        obj = self._m_jar._reader.get_ghost(dbref)
        obj._v_key = doc[self._m_mapping_key]
        obj._v_parent = self
        return obj

class AllItemsMongoContainer(MongoContainer):
    _m_parent_key = None


class SubDocumentMongoContainer(MongoContained, MongoContainer):
    _p_mongo_sub_object = True
