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
import transaction
import bson.dbref
import bson.objectid
import zope.component
from bson.errors import InvalidId
from rwproperty import getproperty, setproperty
from zope.container import contained, sample
from zope.container.interfaces import IContainer

from mongopersist import interfaces
from mongopersist.zope import interfaces as zinterfaces

USE_CONTAINER_CACHE = True

class MongoContained(contained.Contained):

    _v_name = None
    _m_name_attr = None
    _m_name_getter = None
    _m_name_setter = None

    _m_parent_attr = None
    _m_parent_getter = None
    _m_parent_setter = None
    _v_parent = None

    @getproperty
    def __name__(self):
        if self._v_name is None:
            if self._m_name_attr is not None:
                self._v_name = getattr(self, self._m_name_attr, None)
            elif self._m_name_getter is not None:
                self._v_name = self._m_name_getter()
        return self._v_name
    @setproperty
    def __name__(self, value):
        if self._m_name_setter is not None:
            self._m_name_setter(value)
        self._v_name = value

    @getproperty
    def __parent__(self):
        if self._v_parent is None:
            if self._m_parent_attr is not None:
                self._v_parent = getattr(self, self._m_parent_attr, None)
            elif self._m_parent_getter is not None:
                self._v_parent = self._m_parent_getter()
        return self._v_parent
    @setproperty
    def __parent__(self, value):
        if self._m_parent_setter is not None:
            self._m_parent_setter(value)
        self._v_parent = value


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
        obj._v_name = key
        obj._v_parent = self
        return obj

    def get(self, key, default=None):
        '''See interface `IReadContainer`'''
        obj = super(SimpleMongoContainer, self).get(key, default)
        if obj is not default:
            obj._v_name = key
            obj._v_parent = self
        return obj

    def items(self):
        items = super(SimpleMongoContainer, self).items()
        for key, obj in items:
            obj._v_name = key
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
        if not hasattr(self, '_v_mdmp'):
            # If the container is in a Mongo storage hierarchy, then getting
            # the datamanager is easy, otherwise we do an adapter lookup.
            if interfaces.IMongoDataManager.providedBy(self._p_jar):
                return self._p_jar

            # cache result of expensive component lookup
            self._v_mdmp = zope.component.getUtility(
                    interfaces.IMongoDataManagerProvider)

        return self._v_mdmp.get()

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

    def _m_add_items_filter(self, filter):
        for key, value in self._m_get_items_filter().items():
            if key not in filter:
                filter[key] = value

    @property
    def _cache(self):
        if not USE_CONTAINER_CACHE:
            return {}
        txn = transaction.manager.get()
        if not hasattr(txn, '_v_mongo_container_cache'):
            txn._v_mongo_container_cache = {}
        return txn._v_mongo_container_cache.setdefault(self, {})

    @property
    def _cache_complete(self):
        if not USE_CONTAINER_CACHE:
            return False
        txn = transaction.manager.get()
        if not hasattr(txn, '_v_mongo_container_cache_complete'):
            txn._v_mongo_container_cache_complete = {}
        return txn._v_mongo_container_cache_complete.get(self, False)

    def _cache_mark_complete(self):
        txn = transaction.manager.get()
        if not hasattr(txn, '_v_mongo_container_cache_complete'):
            txn._v_mongo_container_cache_complete = {}
        txn._v_mongo_container_cache_complete[self] = True

    def _cache_get_key(self, doc):
        return doc[self._m_mapping_key]

    def _locate(self, obj, doc):
        # Helper method that is only used when locating items that are already
        # in the container and are simply loaded from Mongo.
        if obj.__name__ is None:
            obj._v_name = doc[self._m_mapping_key]
        if obj.__parent__ is None:
            obj._v_parent = self

    def _load_one(self, doc):
        obj = self._cache.get(self._cache_get_key(doc))
        if obj is not None:
            return obj
        # Create a DBRef object and then load the full state of the object.
        dbref = bson.dbref.DBRef(
            self._m_collection, doc['_id'],
            self._m_database or self._m_jar.default_database)
        # Stick the doc into the _latest_states:
        self._m_jar._latest_states[dbref] = doc
        obj = self._m_jar.load(dbref)
        self._locate(obj, doc)
        # Add the object into the local container cache.
        self._cache[obj.__name__] = obj
        return obj

    def __cmp__(self, other):
        # UserDict implements the semantics of implementing comparison of
        # items to determine equality, which is not what we want for a
        # container, so we revert back to the default object comparison.
        return cmp(id(self), id(other))

    def __getitem__(self, key):
        # First check the container cache for the object.
        obj = self._cache.get(key)
        if obj is not None:
            return obj
        if self._cache_complete:
            raise KeyError(key)
        # The cache cannot help, so the item is looked up in the database.
        filter = self._m_get_items_filter()
        filter[self._m_mapping_key] = key
        obj = self.find_one(filter)
        if obj is None:
            raise KeyError(key)
        return obj

    def _real_setitem(self, key, value):
        # Make sure the value is in the database, since we might want
        # to use its oid.
        if value._p_oid is None:
            self._m_jar.insert(value)

        # This call by itself causes the state to change _p_changed to True.
        if self._m_mapping_key is not None:
            setattr(value, self._m_mapping_key, key)
        if self._m_parent_key is not None:
            setattr(value, self._m_parent_key, self._m_get_parent_key_value())

    def __setitem__(self, key, value):
        # When the key is None, we need to determine it.
        if key is None:
            if self._m_mapping_key is None:
                # Make sure the value is in the database, since we might want
                # to use its oid.
                if value._p_oid is None:
                    self._m_jar.insert(value)
                key = unicode(value._p_oid.id)
            else:
                # we have _m_mapping_key, use that attribute
                key = getattr(value, self._m_mapping_key)
        # We want to be as close as possible to using the Zope semantics.
        contained.setitem(self, self._real_setitem, key, value)
        # Also add the item to the container cache.
        self._cache[key] = value

    def add(self, value, key=None):
        # We are already supporting ``None`` valued keys, which prompts the key
        # to be determined here. But people felt that a more explicit
        # interface would be better in this case.
        self[key] = value

    def __delitem__(self, key):
        value = self[key]
        # First remove the parent and name from the object.
        if self._m_mapping_key is not None:
            try:
                delattr(value, self._m_mapping_key)
            except AttributeError:
                # Sometimes we do not control those attributes.
                pass
        if self._m_parent_key is not None:
            try:
                delattr(value, self._m_parent_key)
            except AttributeError:
                # Sometimes we do not control those attributes.
                pass
        # Let's now remove the object from the database.
        if self._m_remove_documents:
            self._m_jar.remove(value)
        # Remove the object from the container cache.
        if USE_CONTAINER_CACHE:
            del self._cache[key]
        # Send the uncontained event.
        contained.uncontained(value, self, key)

    def __contains__(self, key):
        if self._cache_complete:
            return key in self._cache
        return self.raw_find_one(
            {self._m_mapping_key: key}, fields=()) is not None

    def __iter__(self):
        # If the cache contains all objects, we can just return the cache keys.
        if self._cache_complete:
            return iter(self._cache)
        result = self.raw_find(
            {self._m_mapping_key: {'$ne': None}}, fields=(self._m_mapping_key,))
        return iter(doc[self._m_mapping_key] for doc in result)

    def keys(self):
        return list(self.__iter__())

    def iteritems(self):
        # If the cache contains all objects, we can just return the cache keys.
        if self._cache_complete:
            return self._cache.iteritems()
        result = self.raw_find()
        items = [(doc[self._m_mapping_key], self._load_one(doc))
                 for doc in result]
        # Signal the container that the cache is now complete.
        self._cache_mark_complete()
        # Return an iterator of the items.
        return iter(items)

    def raw_find(self, spec=None, *args, **kwargs):
        if spec is None:
            spec = {}
        self._m_add_items_filter(spec)
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
            spec_or_id = {}
        if not isinstance(spec_or_id, dict):
            spec_or_id = {'_id': spec_or_id}
        self._m_add_items_filter(spec_or_id)
        coll = self.get_collection()
        return coll.find_one(spec_or_id, *args, **kwargs)

    def find_one(self, spec_or_id=None, *args, **kwargs):
        doc = self.raw_find_one(spec_or_id, *args, **kwargs)
        if doc is None:
            return None
        return self._load_one(doc)

    def clear(self):
        for key in self.keys():
            del self[key]


class IdNamesMongoContainer(MongoContainer):
    """A container that uses the Mongo ObjectId as the name/key."""
    _m_mapping_key = None

    def __init__(self, collection=None, database=None, parent_key=None):
        super(IdNamesMongoContainer, self).__init__(collection, database, parent_key)


    @property
    def _m_remove_documents(self):
        # Objects must be removed, since removing the _id of a document is not
        # allowed.
        return True

    def _cache_get_key(self, doc):
        return unicode(doc['_id'])

    def _locate(self, obj, doc):
        obj._v_name = unicode(doc['_id'])
        obj._v_parent = self

    def __getitem__(self, key):
        # First check the container cache for the object.
        obj = self._cache.get(key)
        if obj is not None:
            return obj
        if self._cache_complete:
            raise KeyError(key)
        # We do not have a cache entry, so we look up the object.
        try:
            id = bson.objectid.ObjectId(key)
        except InvalidId:
            raise KeyError(key)
        filter = self._m_get_items_filter()
        filter['_id'] = id
        obj = self.find_one(filter)
        if obj is None:
            raise KeyError(key)
        return obj

    def __contains__(self, key):
        # If all objects are loaded, we can look in the local object cache.
        if self._cache_complete:
            return key in self._cache
        # Look in Mongo.
        try:
            id = bson.objectid.ObjectId(key)
        except InvalidId:
            return False
        return self.raw_find_one({'_id': id}, fields=()) is not None

    def __iter__(self):
        # If the cache contains all objects, we can just return the cache keys.
        if self._cache_complete:
            return iter(self._cache)
        # Look up all ids in Mongo.
        result = self.raw_find(fields=None)
        return iter(unicode(doc['_id']) for doc in result)

    def iteritems(self):
        # If the cache contains all objects, we can just return the cache keys.
        if self._cache_complete:
            return self._cache.iteritems()
        # Load all objects from the database.
        result = self.raw_find()
        items = [(unicode(doc['_id']), self._load_one(doc))
                 for doc in result]
        # Signal the container that the cache is now complete.
        self._cache_mark_complete()
        # Return an iterator of the items.
        return iter(items)

    def _real_setitem(self, key, value):
        # We want mongo document ids to be our keys, so pass it to insert(), if
        # key is provided
        if value._p_oid is None:
            self._m_jar.insert(value, bson.objectid.ObjectId(key))

        super(IdNamesMongoContainer, self)._real_setitem(key, value)


class AllItemsMongoContainer(MongoContainer):
    _m_parent_key = None


class SubDocumentMongoContainer(MongoContained, MongoContainer):
    _p_mongo_sub_object = True
