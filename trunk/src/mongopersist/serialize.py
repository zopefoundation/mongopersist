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
"""Object Serialization for Mongo/BSON"""
from __future__ import absolute_import
import copy_reg
import struct

import lru
import persistent.interfaces
import persistent.dict
import persistent.list
import pymongo.binary
import pymongo.dbref
import pymongo.objectid
import types
import zope.interface
from zope.dottedname.resolve import resolve

from mongopersist import interfaces

SERIALIZERS = []
OID_CLASS_LRU = lru.LRUCache(20000)

def p64(v):
    """Pack an integer or long into a 8-byte string"""
    return struct.pack(">Q", v)

def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    return struct.unpack(">Q", v)[0]

def get_dotted_name(obj):
    return obj.__module__+'.'+obj.__name__

class PersistentDict(persistent.dict.PersistentDict):
    _p_mongo_sub_object = True

class PersistentList(persistent.list.PersistentList):
    _p_mongo_sub_object = True


class ObjectSerializer(object):
    zope.interface.implements(interfaces.IObjectSerializer)

    def can_read(self, state):
        raise NotImplementedError

    def read(self, state):
        raise NotImplementedError

    def can_write(self, obj):
        raise NotImplementedError

    def write(self, obj):
        raise NotImplementedError


class ObjectWriter(object):
    zope.interface.implements(interfaces.IObjectWriter)

    def __init__(self, jar):
        self._jar = jar

    def get_collection_name(self, obj):
        db_name = getattr(obj, '_p_mongo_database', self._jar.default_database)
        try:
            coll_name = obj._p_mongo_collection
        except AttributeError:
            return db_name, get_dotted_name(obj.__class__)
        # Make sure that the coll_name to class path mapping is available.
        db = self._jar._conn[self._jar.default_database]
        coll = db[self._jar.name_map_collection]
        map = {'collection': coll_name,
               'database': db_name,
               'path': get_dotted_name(obj.__class__)}
        result = coll.find_one(map)
        if result is None:
            # If there is already a map for this collection, the next map must
            # force the object to store the type.
            result = coll.find({'collection': coll_name,
                                'database': db_name})
            if result.count() > 0:
                setattr(obj, '_p_mongo_store_type', True)
            map['doc_has_type'] = getattr(obj, '_p_mongo_store_type', False)
            coll.save(map)
        return db_name, coll_name

    def get_non_persistent_state(self, obj, seen):
        __traceback_info__ = obj
        # XXX: Look at the pickle library how to properly handle all types and
        # old-style classes with all of the possible pickle extensions.

        # Only non-persistent, custom objects can produce unresolvable
        # circular references.
        if obj in seen:
            raise interfaces.CircularReferenceError(obj)
        # Add the current object to the list of seen objects.
        seen.append(obj)
        # Get the state of the object. Only pickable objects can be reduced.
        reduced = obj.__reduce__()
        # The full object state (item 3) seems to be optional, so let's make
        # sure we handle that case gracefully.
        if len(reduced) == 2:
            factory, args = obj.__reduce__()
            obj_state = {}
        else:
            factory, args, obj_state = reduced
        # We are trying very hard to create a clean Mongo (sub-)document. But
        # we need a little bit of meta-data to help us out later.
        if factory == copy_reg._reconstructor and \
               args == (obj.__class__, object, None):
            # This is the simple case, which means we can produce a nicer
            # Mongo output.
            state = {'_py_type': get_dotted_name(args[0])}
        elif factory == copy_reg.__newobj__ and args == (obj.__class__,):
            # Another simple case for persistent objects that do not want
            # their own document.
            state = {'_py_persistent_type': get_dotted_name(args[0])}
        else:
            state = {'_py_factory': get_dotted_name(factory),
                     '_py_factory_args': self.get_state(args, seen)}
        for name, value in obj_state.items():
            state[name] = self.get_state(value, seen)
        return state

    def get_persistent_state(self, obj, seen):
        __traceback_info__ = obj
        # Persistent sub-objects are stored by reference, the key being
        # (collection name, oid).
        # Getting the collection name is easy, but if we have an unsaved
        # persistent object, we do not yet have an OID. This must be solved by
        # storing the persistent object.
        if obj._p_oid is None:
            dbref = self.store(obj, ref_only=True)
        else:
            db_name, coll_name = self.get_collection_name(obj)
            dbref = obj._p_oid
        # Create the reference sub-document. The _p_type value helps with the
        # deserialization later.
        return dbref

    def get_state(self, obj, seen=None):
        seen = seen or []
        if isinstance(obj, interfaces.MONGO_NATIVE_TYPES):
            # If we have a native type, we'll just use it as the state.
            return obj
        if isinstance(obj, str):
            # In Python 2, strings can be ASCII, encoded unicode or binary
            # data. Unfortunately, BSON cannot handle that. So, if we have a
            # string that cannot be UTF-8 decoded (luckily ASCII is a valid
            # subset of UTF-8), then we use the BSON binary type.
            try:
                obj.decode('utf-8')
                return obj
            except UnicodeError:
                return pymongo.binary.Binary(obj)

        # Some objects might not naturally serialize well and create a very
        # ugly Mongo entry. Thus, we allow custom serializers to be
        # registered, which can encode/decode different types of objects.
        for serializer in SERIALIZERS:
            if serializer.can_write(obj):
                return serializer.write(obj)

        if isinstance(obj, (type, types.ClassType)):
            # We frequently store class and function paths as meta-data, so we
            # need to be able to properly encode those.
            return {'_py_type': 'type',
                    'path': get_dotted_name(obj)}
        if isinstance(obj, (tuple, list, PersistentList)):
            # Make sure that all values within a list are serialized
            # correctly. Also convert any sequence-type to a simple list.
            return [self.get_state(value, seen) for value in obj]
        if isinstance(obj, (dict, PersistentDict)):
            # Same as for sequences, make sure that the contained values are
            # properly serialized.
            # Note: A big constraint in Mongo is that keys must be strings!
            has_non_string_key = False
            data = []
            for key, value in obj.items():
                data.append((key, self.get_state(value, seen)))
                has_non_string_key |= not isinstance(key, basestring)
            if not has_non_string_key:
                # The easy case: all keys are strings:
                return dict(data)
            else:
                # We first need to reduce the keys and then produce a data
                # structure.
                data = [(self.get_state(key), value) for key, value in data]
                return {'dict_data': data}

        if isinstance(obj, persistent.Persistent):
            # Only create a persistent reference, if the object does not want
            # to be a sub-document.
            if not getattr(obj, '_p_mongo_sub_object', False):
                return self.get_persistent_state(obj, seen)
            # This persistent object is a sub-document, so it is treated like
            # a non-persistent object.

        return self.get_non_persistent_state(obj, seen)

    def store(self, obj, ref_only=False):
        db_name, coll_name = self.get_collection_name(obj)
        coll = self._jar._conn[db_name][coll_name]
        if ref_only:
            # We only want to get OID quickly. Trying to reduce the full state
            # might cause infinite recusrion loop. (Example: 2 new objects
            # reference each other.)
            doc = {}
            # Make sure that the object gets saved fully later.
            self._jar.register(obj)
        else:
            # XXX: Handle newargs; see ZODB.serialize.ObjectWriter.serialize
            # Go through each attribute and search for persistent references.
            doc = self.get_state(obj.__getstate__())
        if getattr(obj, '_p_mongo_store_type', False):
            doc['_py_persistent_type'] = get_dotted_name(obj.__class__)
        # If conflict detection is turned on, store a serial number for the
        # document.
        if self._jar.detect_conflicts:
            doc['_py_serial'] = u64(getattr(obj, '_p_serial', 0)) + 1
            obj._p_serial = p64(doc['_py_serial'])

        if obj._p_oid is None:
            doc_id = coll.insert(doc)
            obj._p_jar = self._jar
            obj._p_oid = pymongo.dbref.DBRef(coll_name, doc_id, db_name)
            # Make sure that any other code accessing this object in this
            # session, gets the same instance.
            self._jar._object_cache[doc_id] = obj
        else:
            doc['_id'] = obj._p_oid.id
            coll.save(doc)
        return obj._p_oid


class ObjectReader(object):
    zope.interface.implements(interfaces.IObjectReader)

    def __init__(self, jar):
        self._jar = jar

    def simple_resolve(self, path):
        return resolve(path)

    def resolve(self, dbref):
        try:
            return OID_CLASS_LRU[dbref.id]
        except KeyError:
            pass
        # First we try to resolve the path directly.
        try:
            return self.simple_resolve(dbref.collection)
        except ImportError:
            pass
        # Let's now try to look up the path from the collection to path
        # mapping
        db = self._jar._conn[self._jar.default_database]
        coll = db[self._jar.name_map_collection]
        result = coll.find(
            {'collection': dbref.collection, 'database': dbref.database})
        if result.count() == 0:
            raise ImportError(dbref)
        elif result.count() == 1:
            # Do not add these results to the LRU cache, since the count might
            # change later.
            return self.simple_resolve(result.next()['path'])
        else:
            if dbref.id is None:
                raise ImportError(dbref)
            # Multiple object types are stored in the collection. We have to
            # look at the object to find out the type.
            obj_doc = self._jar._conn[dbref.database][dbref.collection].find_one(
                dbref.id, fields=('_py_persistent_type',))
            if '_py_persistent_type' in obj_doc:
                klass = self.simple_resolve(obj_doc['_py_persistent_type'])
            else:
                # Find the name-map entry where "doc_has_type" is False.
                for name_map_item in result:
                    if not name_map_item['doc_has_type']:
                        klass = self.simple_resolve(name_map_item['path'])
                        break
                else:
                    raise ImportError(path)
            OID_CLASS_LRU[dbref.id] = klass
            return klass

    def get_non_persistent_object(self, state, obj):
        if '_py_type' in state:
            # Handle the simplified case.
            klass = self.simple_resolve(state.pop('_py_type'))
            sub_obj = copy_reg._reconstructor(klass, object, None)
        elif '_py_persistent_type' in state:
            # Another simple case for persistent objects that do not want
            # their own document.
            klass = self.simple_resolve(state.pop('_py_persistent_type'))
            sub_obj = copy_reg.__newobj__(klass)
        else:
            factory = self.simple_resolve(state.pop('_py_factory'))
            factory_args = self.get_object(state.pop('_py_factory_args'), obj)
            sub_obj = factory(*factory_args)
        if len(state):
            sub_obj_state = self.get_object(state, obj)
            if isinstance(sub_obj, persistent.Persistent):
                sub_obj.__setstate__(sub_obj_state)
            else:
                sub_obj.__dict__.update(sub_obj_state)
        if getattr(sub_obj, '_p_mongo_sub_object', False):
            sub_obj._p_mongo_doc_object = obj
            sub_obj._p_jar = self._jar
        return sub_obj

    def get_object(self, state, obj):
        if isinstance(state, pymongo.objectid.ObjectId):
            # The object id is special. Preserve it.
            return state
        if isinstance(state, pymongo.binary.Binary):
            # Binary data in Python 2 is presented as a string. We will
            # convert back to binary when serializing again.
            return str(state)
        if isinstance(state, pymongo.dbref.DBRef):
            # Load a persistent object. Using the get_ghost() method, so that
            # caching is properly applied.
            return self.get_ghost(state)
        if isinstance(state, dict) and state.get('_py_type') == 'type':
            # Convert a simple object reference, mostly classes.
            return self.simple_resolve(state['path'])

        # Give the custom serializers a chance to weigh in.
        for serializer in SERIALIZERS:
            if serializer.can_read(state):
                return serializer.read(state)

        if isinstance(state, dict) and ('_py_factory' in state or \
               '_py_type' in state or '_py_persistent_type' in state):
            # Load a non-persistent object.
            return self.get_non_persistent_object(state, obj)
        if isinstance(state, (tuple, list)):
            # All lists are converted to persistent lists, so that their state
            # changes are noticed. Also make sure that all value states are
            # converted to objects.
            sub_obj = PersistentList(
                [self.get_object(value, obj) for value in state])
            sub_obj._p_mongo_doc_object = obj
            sub_obj._p_jar = self._jar
            return sub_obj
        if isinstance(state, dict):
            # All dictionaries are converted to persistent dictionaries, so
            # that state changes are detected. Also convert all value states
            # to objects.
            # Handle non-string key dicts.
            if 'dict_data' in state:
                items = state['dict_data']
            else:
                items = state.items()
            sub_obj = PersistentDict(
                [(self.get_object(name, obj), self.get_object(value, obj))
                 for name, value in items])
            sub_obj._p_mongo_doc_object = obj
            sub_obj._p_jar = self._jar
            return sub_obj
        return state

    def set_ghost_state(self, obj):
        # Look up the object state by coll_name and oid.
        coll = self._jar._conn[obj._p_oid.database][obj._p_oid.collection]
        doc = coll.find_one({'_id': obj._p_oid.id})
        doc.pop('_id')
        doc.pop('_py_persistent_type', None)
        # Store the serial, if conflict detection is enabled.
        if self._jar.detect_conflicts:
            obj._p_serial = p64(doc.pop('_py_serial', 0))
        # Now convert the document to a proper Python state dict.
        state = self.get_object(doc, obj)
        # Set the state.
        obj.__setstate__(dict(state))

    def get_ghost(self, dbref):
        # If we can, we return the object from cache.
        try:
            return self._jar._object_cache[dbref.id]
        except KeyError:
            pass
        klass = self.resolve(dbref)
        obj = klass.__new__(klass)
        obj._p_jar = self._jar
        obj._p_oid = dbref
        del obj._p_changed
        # Assign the collection after deleting _p_changed, since the attribute
        # is otherwise deleted.
        obj._p_mongo_database = dbref.database
        obj._p_mongo_collection = dbref.collection
        # Adding the object to the cache is very important, so that we get the
        # same object reference throughout the transaction.
        self._jar._object_cache[dbref.id] = obj
        return obj
