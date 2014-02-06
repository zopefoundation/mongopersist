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
import copy
import copy_reg

import bson.dbref
import bson.objectid
import persistent.interfaces
import persistent.dict
import persistent.list
import bson.dbref
import bson.binary
import repoze.lru
import types
import zope.interface
from zope.dottedname.resolve import resolve

from mongopersist import interfaces

IGNORE_IDENTICAL_DOCUMENTS = True
ALWAYS_READ_FULL_DOC = True

SERIALIZERS = []
OID_CLASS_LRU = repoze.lru.LRUCache(20000)
COLLECTIONS_WITH_TYPE = set()
AVAILABLE_NAME_MAPPINGS = set()
PATH_RESOLVE_CACHE = {}


def get_dotted_name(obj):
    return obj.__module__ + '.' + obj.__name__


class PersistentDict(persistent.dict.PersistentDict):
    _p_mongo_sub_object = True

    def __init__(self, data=None, **kwargs):
        # We optimize the case where data is not a dict. The original
        # implementation always created an empty dict, which it then
        # updated. This turned out to be expensive.
        if data is None:
            self.data = {}
        elif isinstance(data, dict):
            self.data = data.copy()
        else:
            self.data = dict(data)
        if len(kwargs):
            self.update(kwargs)

    def __getitem__(self, key):
        # The UserDict supports a __missing__() function, which I have never
        # seen or used before, but it makes the method significantly
        # slower. So let's not do that.
        return self.data[key]


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
        __traceback_info__ = obj
        db_name = getattr(
            obj, '_p_mongo_database',
            self._jar.default_database if self._jar else None)
        try:
            coll_name = obj._p_mongo_collection
        except AttributeError:
            return db_name, get_dotted_name(obj.__class__)
        # If the object writer is run without a datamager, there is no need to
        # try to dump the collection info into the database.
        if self._jar is None:
            return db_name, coll_name
        # Make sure that the coll_name to class path mapping is available.
        # Let's make sure we do the lookup only once, since the info will
        # never change.
        path = get_dotted_name(obj.__class__)
        map = {'collection': coll_name, 'database': db_name, 'path': path}
        map_hash = (db_name, coll_name, path)
        if map_hash in AVAILABLE_NAME_MAPPINGS:
            return db_name, coll_name
        db = self._jar._conn[self._jar.default_database]
        coll = db[self._jar.name_map_collection]
        result = coll.find_one(map)
        if result is None:
            # If there is already a map for this collection, the next map must
            # force the object to store the type.
            result = coll.find({'collection': coll_name,
                                'database': db_name})
            if result.count() > 0:
                setattr(obj.__class__, '_p_mongo_store_type', True)
            map['doc_has_type'] = getattr(obj, '_p_mongo_store_type', False)
            coll.save(map)
            result = map
        # Make sure that derived classes that share a collection know they
        # have to store their type.
        if (result['doc_has_type'] and
            not getattr(obj, '_p_mongo_store_type', False)):
            obj.__class__._p_mongo_store_type = True
        AVAILABLE_NAME_MAPPINGS.add(map_hash)
        return db_name, coll_name

    def get_non_persistent_state(self, obj, seen):
        __traceback_info__ = obj, type(obj)
        # XXX: Look at the pickle library how to properly handle all types and
        # old-style classes with all of the possible pickle extensions.

        # Only non-persistent, custom objects can produce unresolvable
        # circular references.
        if id(obj) in seen:
            raise interfaces.CircularReferenceError(obj)
        # Add the current object to the list of seen objects.
        if not (type(obj) in interfaces.REFERENCE_SAFE_TYPES or
                getattr(obj, '_m_reference_safe', False)):
            seen.append(id(obj))
        # Get the state of the object. Only pickable objects can be reduced.
        reduce_fn = copy_reg.dispatch_table.get(type(obj))
        if reduce_fn is not None:
            reduced = reduce_fn(obj)
        else:
            # XXX: __reduce_ex__
            reduced = obj.__reduce__()
        # The full object state (item 3) seems to be optional, so let's make
        # sure we handle that case gracefully.
        if isinstance(reduced, str):
            # When the reduced state is just a string it represents a name in
            # a module. The module will be extrated from __module__.
            return {'_py_constant': obj.__module__+'.'+reduced}
        if len(reduced) == 2:
            factory, args = reduced
            obj_state = {}
        else:
            factory, args, obj_state = reduced
            if obj_state is None:
                obj_state = {}
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
                     '_py_factory_args': self.get_state(args, obj, seen)}
        for name, value in obj_state.items():
            state[name] = self.get_state(value, obj, seen)
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

    def get_state(self, obj, pobj=None, seen=None):
        seen = seen or []
        if type(obj) in interfaces.MONGO_NATIVE_TYPES:
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
                return bson.binary.Binary(obj)

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

        # We need to make sure that the object's jar and doc-object are
        # set. This is important for the case when a sub-object was just
        # added.
        if getattr(obj, '_p_mongo_sub_object', False):
            if obj._p_jar is None:
                if pobj is not None and pobj._p_jar is not None:
                    obj._p_jar = pobj._p_jar
                obj._p_mongo_doc_object = pobj

        if isinstance(obj, (tuple, list, PersistentList)):
            # Make sure that all values within a list are serialized
            # correctly. Also convert any sequence-type to a simple list.
            return [self.get_state(value, pobj, seen) for value in obj]
        if isinstance(obj, (dict, PersistentDict)):
            # Same as for sequences, make sure that the contained values are
            # properly serialized.
            # Note: A big constraint in Mongo is that keys must be strings!
            has_non_string_key = False
            data = []
            for key, value in obj.items():
                data.append((key, self.get_state(value, pobj, seen)))
                has_non_string_key |= not isinstance(key, basestring)
                if (not isinstance(key, basestring) or '.' in key or '$' in key
                    or '\0' in key):
                    # "Field names cannot contain dots (i.e. .), dollar signs
                    # (i.e. $), or null characters."
                    #   -- http://docs.mongodb.org/manual/reference/limits/
                    has_non_string_key = True
            if not has_non_string_key:
                # The easy case: all keys are strings:
                return dict(data)
            else:
                # We first need to reduce the keys and then produce a data
                # structure.
                data = [(self.get_state(key, pobj), value)
                        for key, value in data]
                return {'dict_data': data}

        if isinstance(obj, persistent.Persistent):
            # Only create a persistent reference, if the object does not want
            # to be a sub-document.
            if not getattr(obj, '_p_mongo_sub_object', False):
                return self.get_persistent_state(obj, seen)
            # This persistent object is a sub-document, so it is treated like
            # a non-persistent object.

        return self.get_non_persistent_state(obj, seen)

    def get_full_state(self, obj):
        doc = self.get_state(obj.__getstate__(), obj)
        # Add a persistent type info, if necessary.
        if getattr(obj, '_p_mongo_store_type', False):
            doc['_py_persistent_type'] = get_dotted_name(obj.__class__)
        # A hook, so that the conflict handler can modify the state document
        # if needed.
        self._jar.conflict_handler.on_before_store(obj, doc)
        # Add the object id.
        if obj._p_oid is not None:
            doc['_id'] = obj._p_oid.id
        # Return the full state document
        return doc

    def store(self, obj, ref_only=False, id=None):
        __traceback_info__ = (obj, ref_only)

        db_name, coll_name = self.get_collection_name(obj)
        coll = self._jar.get_collection(db_name, coll_name)
        if ref_only:
            # We only want to get OID quickly. Trying to reduce the full state
            # might cause infinite recursion loop. (Example: 2 new objects
            # reference each other.)
            doc = {}
            # Make sure that the object gets saved fully later.
            self._jar.register(obj)
        else:
            # XXX: Handle newargs; see ZODB.serialize.ObjectWriter.serialize
            # Go through each attribute and search for persistent references.
            doc = self.get_state(obj.__getstate__(), obj)

        if getattr(obj, '_p_mongo_store_type', False):
            doc['_py_persistent_type'] = get_dotted_name(obj.__class__)

        # A hook, so that the conflict handler can modify the state document
        # if needed.
        self._jar.conflict_handler.on_before_store(obj, doc)

        stored = False
        if obj._p_oid is None:
            if id is not None:
                doc['_id'] = id
            doc_id = coll.insert(doc)
            stored = True
            obj._p_jar = self._jar
            obj._p_oid = bson.dbref.DBRef(coll_name, doc_id, db_name)
            # Make sure that any other code accessing this object in this
            # session, gets the same instance.
            self._jar._object_cache[hash(obj._p_oid)] = obj
        else:
            doc['_id'] = obj._p_oid.id
            # We only want to store a new version of the document, if it is
            # different. We have to delegate that task to the conflict
            # handler, since it might know about meta-fields that need to be
            # ignored.
            orig_doc = self._jar._latest_states.get(obj._p_oid)
            if (not IGNORE_IDENTICAL_DOCUMENTS or
                not self._jar.conflict_handler.is_same(obj, orig_doc, doc)):
                coll.save(doc)
                stored = True

        if stored:
            # Make sure that the doc is added to the latest states.
            self._jar._latest_states[obj._p_oid] = doc

            # A hook, so that the conflict handler can modify the object or state
            # document after an object was stored.
            self._jar.conflict_handler.on_after_store(obj, doc)

        return obj._p_oid


class ObjectReader(object):
    zope.interface.implements(interfaces.IObjectReader)

    def __init__(self, jar):
        self._jar = jar
        self._single_map_cache = {}
        self.preferPersistent = True

    def simple_resolve(self, path):
        # We try to look up the klass from a cache. The important part here is
        # that we also cache lookup failures as None, since they actually
        # happen more frequently than a hit due to an optimization in the
        # resolve() function.
        try:
            klass = PATH_RESOLVE_CACHE[path]
        except KeyError:
            try:
                klass = resolve(path)
            except ImportError:
                PATH_RESOLVE_CACHE[path] = klass = None
            else:
                PATH_RESOLVE_CACHE[path] = klass
        if klass is None:
            raise ImportError(path)
        return klass

    def resolve(self, dbref):
        __traceback_info__ = dbref
        # 1. Check the global oid-based lookup cache. Use the hash of the id,
        #    since otherwise the comparison is way too expensive.
        klass = OID_CLASS_LRU.get(hash(dbref))
        if klass is not None:
            return klass
        # 2. Check the transient single map entry lookup cache.
        try:
            return self._single_map_cache[(dbref.database, dbref.collection)]
        except KeyError:
            pass
        # 3. If we have found the type within the document for a collection
        #    before, let's try again. This will only hit, if we have more than
        #    one type for the collection, otherwise the single map entry
        #    lookup failed.
        coll_key = (dbref.database, dbref.collection)
        if coll_key in COLLECTIONS_WITH_TYPE:
            if dbref in self._jar._latest_states:
                obj_doc = self._jar._latest_states[dbref]
            elif ALWAYS_READ_FULL_DOC:
                obj_doc = self._jar.get_collection(
                    dbref.database, dbref.collection).find_one(dbref.id)
                self._jar._latest_states[dbref] = obj_doc
            else:
                obj_doc = self._jar\
                    .get_collection(dbref.database, dbref.collection)\
                    .find_one(dbref.id, fields=('_py_persistent_type',))
            #if obj_doc is None:
            #    # There is no document for this reference in the database.
            #    raise ImportError(dbref)
            if '_py_persistent_type' in obj_doc:
                klass = self.simple_resolve(obj_doc['_py_persistent_type'])
                OID_CLASS_LRU.put(hash(dbref), klass)
                return klass
        # 4. Try to resolve the path directly. We want to do this optimization
        #    after all others, because trying it a lot is very expensive.
        try:
            return self.simple_resolve(dbref.collection)
        except ImportError:
            pass
        # 5. No simple hits, so we have to do some leg work.
        # Let's now try to look up the path from the collection to path
        # mapping
        db = self._jar._conn[self._jar.default_database]
        coll = db[self._jar.name_map_collection]
        result = tuple(coll.find(
            {'collection': dbref.collection, 'database': dbref.database}))
        # Calling count() on a query result causes another database
        # access. Since the result sets should be typically very small, let's
        # load them all.
        count = len(result)
        if count == 0:
            raise ImportError(dbref)
        elif count == 1:
            # Do not add these results to the LRU cache, since the count might
            # change later. But storing it for the length of the transaction
            # is fine, which is really useful if you load a lot of objects of
            # the same type.
            klass = self.simple_resolve(result[0]['path'])
            self._single_map_cache[(dbref.database, dbref.collection)] = klass
            return klass
        else:
            if dbref.id is None:
                raise ImportError(dbref)
            # Multiple object types are stored in the collection. We have to
            # look at the object to find out the type.
            if dbref in self._jar._latest_states:
                # Optimization: If we have the latest state, then we just get
                # this object document. This is used for fast loading or when
                # resolving the same object path a second time. (The latter
                # should never happen due to the object cache.)
                obj_doc = self._jar._latest_states[dbref]
            elif ALWAYS_READ_FULL_DOC:
                # Optimization: Read the entire doc and stick it in the right
                # place so that unghostifying the object later will not cause
                # another database access.
                obj_doc = self._jar\
                    .get_collection(dbref.database, dbref.collection)\
                    .find_one(dbref.id)
                self._jar._latest_states[dbref] = obj_doc
            else:
                obj_doc = self._jar\
                    .get_collection(dbref.database, dbref.collection)\
                    .find_one(dbref.id, fields=('_py_persistent_type',))
            if '_py_persistent_type' in obj_doc:
                COLLECTIONS_WITH_TYPE.add(coll_key)
                klass = self.simple_resolve(obj_doc['_py_persistent_type'])
            else:
                # Find the name-map entry where "doc_has_type" is False.
                # Note: This case is really inefficient and does not allow any
                # optimization. It should be avoided as much as possible.
                for name_map_item in result:
                    if not name_map_item['doc_has_type']:
                        klass = self.simple_resolve(name_map_item['path'])
                        break
                else:
                    raise ImportError(dbref)
            OID_CLASS_LRU.put(hash(dbref), klass)
            return klass

    def get_non_persistent_object(self, state, obj):
        if '_py_constant' in state:
            return self.simple_resolve(state.pop('_py_constant'))
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
                # This is a persistent sub-object -- mark it as such. Otherwise
                # we risk to store this object in its own collection next time.
                sub_obj._p_mongo_sub_object = True
            else:
                sub_obj.__dict__.update(sub_obj_state)
        if getattr(sub_obj, '_p_mongo_sub_object', False):
            sub_obj._p_mongo_doc_object = obj
            sub_obj._p_jar = self._jar
        return sub_obj

    def get_object(self, state, obj):
        if isinstance(state, bson.objectid.ObjectId):
            # The object id is special. Preserve it.
            return state
        if isinstance(state, bson.binary.Binary):
            # Binary data in Python 2 is presented as a string. We will
            # convert back to binary when serializing again.
            return str(state)
        if isinstance(state, bson.dbref.DBRef):
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

        if isinstance(state, dict) and (
            '_py_factory' in state
            or '_py_constant' in state
            or '_py_type' in state
            or '_py_persistent_type' in state):
            # Load a non-persistent object.
            return self.get_non_persistent_object(state, obj)
        if isinstance(state, (tuple, list)):
            # All lists are converted to persistent lists, so that their state
            # changes are noticed. Also make sure that all value states are
            # converted to objects.
            sub_obj = [self.get_object(value, obj) for value in state]
            if self.preferPersistent:
                sub_obj = PersistentList(sub_obj)
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
            sub_obj = dict(
                [(self.get_object(name, obj), self.get_object(value, obj))
                 for name, value in items])
            if self.preferPersistent:
                sub_obj = PersistentDict(sub_obj)
                sub_obj._p_mongo_doc_object = obj
                sub_obj._p_jar = self._jar
            return sub_obj
        return state

    def set_ghost_state(self, obj, doc=None):
        __traceback_info__ = (obj, doc)
        # Check whether the object state was stored on the object itself.
        if doc is None:
            doc = getattr(obj, '_p_mongo_state', None)
        # Look up the object state by coll_name and oid.
        if doc is None:
            coll = self._jar.get_collection(
                obj._p_oid.database, obj._p_oid.collection)
            doc = coll.find_one({'_id': obj._p_oid.id})
        # Check that we really have a state doc now.
        if doc is None:
            raise ImportError(obj._p_oid)
        # Create a copy of the doc, so that we can modify it.
        state_doc = copy.deepcopy(doc)
        # Remove unwanted attributes.
        state_doc.pop('_id')
        state_doc.pop('_py_persistent_type', None)
        # Allow the conflict handler to modify the object or state document
        # before it is set on the object.
        self._jar.conflict_handler.on_before_set_state(obj, state_doc)
        # Now convert the document to a proper Python state dict.
        state = dict(self.get_object(state_doc, obj))
        # Now store the original state. It is assumed that the state dict is
        # not modified later.
        # Make sure that we never set the original state multiple times, even
        # if reassigning the state within the same transaction. Otherwise we
        # can never fully undo a transaction.
        if obj._p_oid not in self._jar._original_states:
            self._jar._original_states[obj._p_oid] = doc
            # Sometimes this method is called to update the object state
            # before storage. Only update the latest states when the object is
            # originally loaded.
            self._jar._latest_states[obj._p_oid] = doc
        # Set the state.
        obj.__setstate__(state)

    def get_ghost(self, dbref, klass=None):
        # If we can, we return the object from cache.
        try:
            return self._jar._object_cache[hash(dbref)]
        except KeyError:
            pass
        if klass is None:
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
        self._jar._object_cache[hash(dbref)] = obj
        return obj
