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
import persistent
import pymongo
import pymongo.dbref
import transaction
import zope.interface

from mongopersist import interfaces, serialize

def create_conflict_error(obj, new_doc):
    return interfaces.ConflictError(
        None, obj,
        (new_doc.get('_py_serial', 0), serialize.u64(obj._p_serial)))


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
        self._collection_inst = db[self.collection]

    def __getitem__(self, key):
        doc = self._collection_inst.find_one({'name': key})
        if doc is None:
            raise KeyError(key)
        return self._jar.load(doc['ref'])

    def __setitem__(self, key, value):
        dbref = self._jar.dump(value)
        if self.get(key) is not None:
            del self[key]
        doc = {'ref': dbref, 'name': key}
        self._collection_inst.insert(doc)

    def __delitem__(self, key):
        doc = self._collection_inst.find_one({'name': key})
        coll = self._jar._conn[doc['ref'].database][doc['ref'].collection]
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

    def dump(self, obj):
        return self._writer.store(obj)

    def load(self, dbref):
        return self._reader.get_ghost(dbref)

    def reset(self):
        root = self.root
        self.__init__(self._conn)
        self.root = root

    def setstate(self, obj):
        # When reading a state from Mongo, we also need to join the
        # transaction, because we keep an active object cache that gets stale
        # after the transaction is complete and must be cleaned.
        if self._needs_to_join:
            self.transaction_manager.get().join(self)
            self._needs_to_join = False
        self._reader.set_ghost_state(obj)
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
        self.reset()

    def commit(self, transaction):
        if not self.detect_conflicts:
            return
        # Check each modified object to see whether Mongo has a new version of
        # the object.
        for obj in self._registered_objects:
            # This object is not even added to the database yet, so there
            # cannot be a conflict.
            if obj._p_oid is None:
                continue
            db_name, coll_name = self._writer.get_collection_name(obj)
            coll = self._conn[db_name][coll_name]
            new_doc = coll.find_one(obj._p_oid.id, fields=('_py_serial',))
            if new_doc is None:
                continue
            if new_doc.get('_py_serial', 0) != serialize.u64(obj._p_serial):
                raise self.conflict_error_factory(obj, new_doc)

    def tpc_begin(self, transaction):
        pass

    def tpc_vote(self, transaction):
        pass

    def tpc_finish(self, transaction):
        written = []
        for obj in self._registered_objects:
            if getattr(obj, '_p_mongo_sub_object', False):
                obj = obj._p_mongo_doc_object
            if obj in written:
                continue
            self._writer.store(obj)
            written.append(obj)
        self.reset()

    def tpc_abort(self, transaction):
        self.abort(transaction)

    def sortKey(self):
        return ('MongoDataManager', 0)
