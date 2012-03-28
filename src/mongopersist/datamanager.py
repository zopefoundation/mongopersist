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
"""Mongo Persistent Data Manager"""
from __future__ import absolute_import
import UserDict
import logging
import persistent
import pymongo
import pymongo.dbref
import transaction
import traceback
import sys
import zope.interface

from zope.exceptions import exceptionformatter
from mongopersist import interfaces, serialize

COLLECTION_LOG = logging.getLogger('mongopersist.collection')

def create_conflict_error(obj, new_doc):
    return interfaces.ConflictError(
        None, obj,
        (new_doc.get('_py_serial', 0), serialize.u64(obj._p_serial)))

def process_spec(collection, spec):
    try:
        adapter = interfaces.IMongoSpecProcessor(None)
    except TypeError:
        # by default nothing is registered, handle that case
        return spec

    return adapter.process(collection, spec)

class FlushDecorator(object):

    def __init__(self, datamanager, function):
        self.datamanager = datamanager
        self.function = function

    def __call__(self, *args, **kwargs):
        self.datamanager.flush()
        return self.function(*args, **kwargs)

class ProcessSpecDecorator(object):

    def __init__(self, collection, function):
        self.collection = collection
        self.function = function

    def __call__(self, *args, **kwargs):
        if args:
            args = (process_spec(self.collection, args[0]),) + args[1:]
        # find()
        if 'spec' in kwargs:
            kwargs['spec'] = process_spec(self.collection, kwargs['spec'])
        # find_one()
        elif 'spec_or_id' in kwargs:
            kwargs['spec_or_id'] = process_spec(
                self.collection, kwargs['spec_or_id'])
        # find_and_modify()
        elif 'query' in kwargs:
            kwargs['query'] = process_spec(self.collection, kwargs['query'])
        return self.function(*args, **kwargs)

class LoggingDecorator(object):

    # these are here to be easily patched
    ADDTB = True
    TB_LIMIT = 10 # 10 should be sufficient to figure

    def __init__(self, collection, function):
        self.collection = collection
        self.function = function

    def __call__(self, *args, **kwargs):
        if self.ADDTB:
            try:
                raise ValueError('boom')
            except:
                # we need here exceptionformatter, otherwise __traceback_info__
                # is not added
                tb = ''.join(exceptionformatter.extract_stack(
                    sys.exc_info()[2].tb_frame.f_back, limit=self.TB_LIMIT))
        else:
            tb = '<omitted>'

        COLLECTION_LOG.debug(
            "collection: %s.%s %s,\n args:%r,\n kwargs:%r, \n tb:\n%s",
            self.collection.database.name, self.collection.name,
            self.function.__name__, args, kwargs, tb)

        return self.function(*args, **kwargs)

class CollectionWrapper(object):

    LOGGED_METHODS = ['insert', 'update', 'remove', 'save',
                      'find_and_modify', 'find_one', 'find', 'count']
    QUERY_METHODS = ['group', 'map_reduce', 'inline_map_reduce', 'find_one',
                     'find', 'find_and_modify']
    PROCESS_SPEC_METHODS = ['find_and_modify', 'find_one', 'find']

    def __init__(self, collection, datamanager):
        self.__dict__['collection'] = collection
        self.__dict__['_datamanager'] = datamanager

    def __getattr__(self, name):
        attr = getattr(self.collection, name)
        if name in self.LOGGED_METHODS:
            attr = LoggingDecorator(self.collection, attr)
        if name in self.QUERY_METHODS:
            attr = FlushDecorator(self._datamanager, attr)
        if name in self.PROCESS_SPEC_METHODS:
            attr = ProcessSpecDecorator(self.collection, attr)
        return attr

    def __setattr__(self, name, value):
        setattr(self.collection, name, value)

    def __delattr__(self, name):
        delattr(self.collection, name)


class Root(UserDict.DictMixin):

    database='mongopersist'
    collection = 'persistence_root'

    def __init__(self, jar, database=None, collection=None):
        self._jar = jar
        if database is not None:
            self.database = database
        if collection is not None:
            self.collection = collection
        db = self._jar._conn[self.database]
        self._collection_inst = CollectionWrapper(db[self.collection], jar)

    def __getitem__(self, key):
        doc = self._collection_inst.find_one({'name': key})
        if doc is None:
            raise KeyError(key)
        return self._jar.load(doc['ref'])

    def __setitem__(self, key, value):
        dbref = self._jar.insert(value)
        if self.get(key) is not None:
            del self[key]
        doc = {'ref': dbref, 'name': key}
        self._collection_inst.insert(doc)

    def __delitem__(self, key):
        doc = self._collection_inst.find_one({'name': key})
        coll = self._jar.get_collection(
            doc['ref'].database, doc['ref'].collection)
        coll.remove(doc['ref'].id)
        self._collection_inst.remove({'name': key})

    def keys(self):
        return [doc['name'] for doc in self._collection_inst.find()]


class MongoDataManager(object):
    zope.interface.implements(interfaces.IMongoDataManager)

    detect_conflicts = False
    default_database = 'mongopersist'
    name_map_collection = 'persistence_name_map'
    conflict_error_factory = staticmethod(create_conflict_error)

    def __init__(self, conn, detect_conflicts=None, default_database=None,
                 root_database=None, root_collection=None,
                 name_map_collection=None, conflict_error_factory=None):
        self._conn = conn
        self._reader = serialize.ObjectReader(self)
        self._writer = serialize.ObjectWriter(self)
        self._registered_objects = []
        self._loaded_objects = []
        self._inserted_objects = []
        self._removed_objects = []
        self._original_states = {}
        self._needs_to_join = True
        self._object_cache = {}
        self.annotations = {}
        if detect_conflicts is not None:
            self.detect_conflicts = detect_conflicts
        if default_database is not None:
            self.default_database = default_database
        if name_map_collection is not None:
            self.name_map_collection = name_map_collection
        if conflict_error_factory is not None:
            self.conflict_error_factory = conflict_error_factory
        self.transaction_manager = transaction.manager
        self.root = Root(self, root_database, root_collection)

    def _get_collection(self, db_name, coll_name):
        return self._conn[db_name][coll_name]

    def _get_collection_from_object(self, obj):
        db_name, coll_name = self._writer.get_collection_name(obj)
        return self._get_collection(db_name, coll_name)

    def _check_conflicts(self):
        if not self.detect_conflicts:
            return
        # Check each modified object to see whether Mongo has a new version of
        # the object.
        for obj in self._registered_objects:
            # This object is not even added to the database yet, so there
            # cannot be a conflict.
            if obj._p_oid is None:
                continue
            coll = self._get_collection_from_object(obj)
            new_doc = coll.find_one(obj._p_oid.id, fields=('_py_serial',))
            if new_doc is None:
                continue
            if new_doc.get('_py_serial', 0) != serialize.u64(obj._p_serial):
                raise self.conflict_error_factory(obj, new_doc)

    def _flush_objects(self):
        # Now write every registered object, but make sure we write each
        # object just once.
        written = []
        for obj in self._registered_objects:
            if getattr(obj, '_p_mongo_sub_object', False):
                # Make sure we write the object representing a document in a
                # collection and not a sub-object.
                obj = obj._p_mongo_doc_object
            if obj in written:
                continue
            self._writer.store(obj)
            written.append(obj)

    def get_collection(self, db_name, coll_name):
        return CollectionWrapper(self._get_collection(db_name, coll_name), self)

    def get_collection_from_object(self, obj):
        return CollectionWrapper(self._get_collection_from_object(obj), self)

    def dump(self, obj):
        res = self._writer.store(obj)
        if obj in self._registered_objects:
            obj._p_changed = False
            self._registered_objects.remove(obj)
        return res

    def load(self, dbref):
        return self._reader.get_ghost(dbref)

    def reset(self):
        root = self.root
        self.__init__(self._conn)
        self.root = root

    def flush(self):
        # Check for conflicts.
        self._check_conflicts()
        # Now write every registered object, but make sure we write each
        # object just once.
        self._flush_objects()
        # Let's now reset all objects as if they were not modified:
        for obj in self._registered_objects:
            obj._p_changed = False
        self._registered_objects = []

    def insert(self, obj):
        if obj._p_oid is not None:
            raise ValueError('Object has already an OID.', obj)
        res = self._writer.store(obj)
        obj._p_changed = False
        self._object_cache[obj._p_oid] = obj
        self._inserted_objects.append(obj)
        return res

    def remove(self, obj):
        if obj._p_oid is None:
            raise ValueError('Object does not have OID.', obj)
        # Edge case: The object was just added in this transaction.
        if obj in self._inserted_objects:
            self._inserted_objects.remove(obj)
            return
        # If the object is still in the ghost state, let's load it, so that we
        # have the state in case we abort the transaction later.
        if obj._p_changed is None:
            self.setstate(obj)
        # Now we remove the object from Mongo.
        coll = self.get_collection_from_object(obj)
        coll.remove({'_id': obj._p_oid.id})
        self._removed_objects.append(obj)
        # Just in case the object was modified before removal, let's remove it
        # from the modification list:
        if obj._p_changed:
            self._registered_objects.remove(obj)
        # We are not doing anything fancy here, since the object might be
        # added again with some different state.

    def setstate(self, obj, doc=None):
        # When reading a state from Mongo, we also need to join the
        # transaction, because we keep an active object cache that gets stale
        # after the transaction is complete and must be cleaned.
        if self._needs_to_join:
            self.transaction_manager.get().join(self)
            self._needs_to_join = False
        self._reader.set_ghost_state(obj, doc)
        self._loaded_objects.append(obj)

    def oldstate(self, obj, tid):
        # I cannot find any code using this method. Also, since we do not keep
        # version history, we always raise an error.
        raise KeyError(tid)

    def register(self, obj):
        if self._needs_to_join:
            self.transaction_manager.get().join(self)
            self._needs_to_join = False

        if obj is not None and obj not in self._registered_objects:
            self._registered_objects.append(obj)

    def abort(self, transaction):
        # Aborting the transaction requires three steps:
        # 1. Remove any inserted objects.
        for obj in self._inserted_objects:
            coll = self.get_collection_from_object(obj)
            coll.remove({'_id': obj._p_oid.id})
        # 2. Re-insert any removed objects.
        for obj in self._removed_objects:
            coll = self.get_collection_from_object(obj)
            coll.insert(self._original_states[obj._p_oid])
            del self._original_states[obj._p_oid]
        # 3. Reset any changed states.
        for db_ref, state in self._original_states.items():
            coll = self.get_collection(db_ref.database, db_ref.collection)
            coll.update({'_id': db_ref.id}, state, True)
        self.reset()

    def commit(self, transaction):
        self._check_conflicts()

    def tpc_begin(self, transaction):
        pass

    def tpc_vote(self, transaction):
        pass

    def tpc_finish(self, transaction):
        self._flush_objects()
        self.reset()

    def tpc_abort(self, transaction):
        self.abort(transaction)

    def sortKey(self):
        return ('MongoDataManager', 0)
