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
"""Mongo Persistence Serializeation Tests"""
import datetime
import doctest
import persistent
import pprint

from pymongo import binary, dbref, objectid

from mongopersist import testing, serialize

class Top(persistent.Persistent):
    _p_mongo_collection = 'Top'

def create_top(name):
    top = Top()
    top.name = name
    return top

class Top2(Top):
    pass

class Tier2(persistent.Persistent):
    _p_mongo_sub_object = True

class Foo(persistent.Persistent):
    _p_mongo_collection = 'Foo'

class Anything(persistent.Persistent):
    pass

class Simple(object):
    pass

def doctest_ObjectSerializer():
    """Test the abstract ObjectSerializer class.

    Object serializers are hooks into the serialization process to allow
    better serialization for particular objects. For example, the result of
    reducing a datetime.date object is a short, optimized binary string. This
    representation might be optimal for pickles, but is really aweful for
    Mongo, since it does not allow querying for dates. An object serializer
    can be used to use a better representation, such as the date ordinal
    number.

      >>> os = serialize.ObjectSerializer()

    So here are the methods that must be implemented by an object serializer:

      >>> os.can_read({})
      Traceback (most recent call last):
      ...
      NotImplementedError

      >>> os.read({})
      Traceback (most recent call last):
      ...
      NotImplementedError

      >>> os.can_write(object())
      Traceback (most recent call last):
      ...
      NotImplementedError

      >>> os.write(object())
      Traceback (most recent call last):
      ...
      NotImplementedError
    """

def doctest_ObjectWriter_get_collection_name():
    """ObjectWriter: get_collection_name()

    This method determines the collection name and database for a given
    object. It can either be specified via '_p_mongo_collection' or is
    determined from the class path. When the collection name is specified, the
    mapping from collection name to class path is stored.

      >>> print tuple(conn[DBNAME][dm.name_map_collection].find())
      ()

      >>> writer = serialize.ObjectWriter(dm)
      >>> writer.get_collection_name(Anything())
      ('mongopersist_test', 'mongopersist.tests.test_serialize.Anything')

      >>> top = Top()
      >>> writer.get_collection_name(top)
      ('mongopersist_test', 'Top')

      >>> print tuple(conn[DBNAME][dm.name_map_collection].find())
      ({u'path': u'mongopersist.tests.test_serialize.Top',
        u'doc_has_type': False,
        u'_id': ObjectId('4eb19f9937a08e27b7000000'),
        u'collection': u'Top',
        u'database': u'mongopersist_test'},)

      >>> getattr(top, '_p_mongo_store_type', None)

    When classes use inheritance, it often happens that all sub-objects share
    the same collection. However, only one can have an entry in our mapping
    table to avoid non-unique answers. Thus we require all sub-types after the
    first one to store their typing providing a hint for deseriealization:

      >>> top2 = Top2()
      >>> writer.get_collection_name(top2)
      ('mongopersist_test', 'Top')

      >>> pprint.pprint(tuple(conn[DBNAME][dm.name_map_collection].find()))
      ({u'_id': ObjectId('4eb1b5ab37a08e2f06000000'),
        u'collection': u'Top',
        u'database': u'mongopersist_test',
        u'doc_has_type': False,
        u'path': u'mongopersist.tests.test_serialize.Top'},
       {u'_id': ObjectId('4eb1b5ab37a08e2f06000001'),
        u'collection': u'Top',
        u'database': u'mongopersist_test',
        u'doc_has_type': True,
        u'path': u'mongopersist.tests.test_serialize.Top2'})

      >>> getattr(top2, '_p_mongo_store_type', None)
      True
    """

def doctest_ObjectWriter_get_non_persistent_state():
    r"""ObjectWriter: get_non_persistent_state()

    This method produces a proper reduced state for custom, non-persistent
    objects.

      >>> writer = serialize.ObjectWriter(dm)

    A simple new-style class:

      >>> class This(object):
      ...     def __init__(self, num):
      ...         self.num = num

      >>> this = This(1)
      >>> writer.get_non_persistent_state(this, [])
      {'num': 1, '_py_type': '__main__.This'}

    A simple old-style class:

      >>> class That(object):
      ...     def __init__(self, num):
      ...         self.num = num

      >>> that = That(1)
      >>> writer.get_non_persistent_state(that, [])
      {'num': 1, '_py_type': '__main__.That'}

    The method also handles persistent classes that do not want their own
    document:

      >>> top = Top()
      >>> writer.get_non_persistent_state(top, [])
      {'_py_persistent_type': 'mongopersist.tests.test_serialize.Top'}

    And then there are the really weird cases:

      >>> writer.get_non_persistent_state(datetime.date(2011, 11, 1), [])
      {'_py_factory': 'datetime.date',
       '_py_factory_args': [Binary('\x07\xdb\x0b\x01', 0)]}

    Circular object references cause an error:

      >>> writer.get_non_persistent_state(this, [this])
      Traceback (most recent call last):
      ...
      CircularReferenceError: <__main__.This object at 0x3051550>
    """

def doctest_ObjectWriter_get_persistent_state():
    r"""ObjectWriter: get_persistent_state()

    This method produces a proper reduced state for a persistent object, which
    is basically a Mongo DBRef.

      >>> writer = serialize.ObjectWriter(dm)

      >>> foo = Foo()
      >>> foo._p_oid
      >>> list(conn[DBNAME]['Foo'].find())
      []

      >>> writer.get_persistent_state(foo, [])
      DBRef('Foo', ObjectId('4eb1a87f37a08e29ff000002'), 'mongopersist_test')

      >>> foo._p_oid
      DBRef('Foo', ObjectId('4eb1a87f37a08e29ff000002'), 'mongopersist_test')
      >>> pprint.pprint(list(conn[DBNAME]['Foo'].find()))
      [{u'_id': ObjectId('4eb1a96c37a08e2a7b000002')}]

    The next time the object simply returns its reference:

      >>> writer.get_persistent_state(foo, [])
      DBRef('Foo', ObjectId('4eb1a87f37a08e29ff000002'), 'mongopersist_test')
      >>> pprint.pprint(list(conn[DBNAME]['Foo'].find()))
      [{u'_id': ObjectId('4eb1a96c37a08e2a7b000002')}]
    """


def doctest_ObjectWriter_get_state_MONGO_NATIVE_TYPES():
    """ObjectWriter: get_state(): Mongo-native Types

      >>> writer = serialize.ObjectWriter(None)
      >>> writer.get_state(1)
      1
      >>> writer.get_state(1.0)
      1.0
      >>> writer.get_state(u'Test')
      u'Test'
      >>> writer.get_state(datetime.datetime(2011, 11, 1, 12, 0, 0))
      datetime.datetime(2011, 11, 1, 12, 0, 0)
      >>> print writer.get_state(None)
      None
      >>> writer.get_state(objectid.ObjectId('4e7ddf12e138237403000000'))
      ObjectId('4e7ddf12e138237403000000')
      >>> writer.get_state(dbref.DBRef('4e7ddf12e138237403000000', 'test'))
      DBRef('4e7ddf12e138237403000000', 'test')
    """

def doctest_ObjectWriter_get_state_types():
    """ObjectWriter: get_state(): types (type, class)

      >>> writer = serialize.ObjectWriter(None)
      >>> writer.get_state(Top)
      {'path': 'mongopersist.tests.test_serialize.Top', '_py_type': 'type'}
      >>> writer.get_state(str)
      {'path': '__builtin__.str', '_py_type': 'type'}
    """

def doctest_ObjectWriter_get_state_sequences():
    """ObjectWriter: get_state(): sequences (tuple, list, PersistentList)

    We convert any sequence into a simple list, since Mongo supports that
    type natively. But also reduce any sub-objects.

      >>> class Number(object):
      ...     def __init__(self, num):
      ...         self.num = num

      >>> writer = serialize.ObjectWriter(None)
      >>> writer.get_state((1, '2', Number(3)))
      [1, '2', {'num': 3, '_py_type': '__main__.Number'}]
      >>> writer.get_state([1, '2', Number(3)])
      [1, '2', {'num': 3, '_py_type': '__main__.Number'}]
    """

def doctest_ObjectWriter_get_state_mappings():
    """ObjectWriter: get_state(): mappings (dict, PersistentDict)

    We convert any mapping into a simple dict, since Mongo supports that
    type natively. But also reduce any sub-objects.

      >>> class Number(object):
      ...     def __init__(self, num):
      ...         self.num = num

      >>> writer = serialize.ObjectWriter(None)
      >>> writer.get_state({'1': 1, '2': '2', '3': Number(3)})
      {'1': 1, '3': {'num': 3, '_py_type': '__main__.Number'}, '2': '2'}

    Unfortunately, Mongo only supports text keys. So whenever we have non-text
    keys, we need to create a less natural, but consistent structure:

      >>> writer.get_state({1: 'one', 2: 'two', 3: 'three'})
      {'dict_data': [(1, 'one'), (2, 'two'), (3, 'three')]}
    """

def doctest_ObjectWriter_get_state_Persistent():
    """ObjectWriter: get_state(): Persistent objects

      >>> writer = serialize.ObjectWriter(dm)

      >>> top = Top()
      >>> writer.get_state(top)
      DBRef('Top', ObjectId('4eb1aede37a08e2c8d000004'), 'mongopersist_test')

    But a persistent object can declare that it does not want a separate
    document:

      >>> top2 = Top()
      >>> top2._p_mongo_sub_object = True
      >>> writer.get_state(top2)
      {'_py_persistent_type': 'mongopersist.tests.test_serialize.Top'}
    """

def doctest_ObjectWriter_store():
    """ObjectWriter: store()

      >>> writer = serialize.ObjectWriter(dm)

    Simply store an object:

      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      []

      >>> top = Top()
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1b16537a08e2d1a000001'), 'mongopersist_test')
      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      [{u'_id': ObjectId('4eb1b17937a08e2d29000001')}]

    Now that we have an object, storing an object simply means updating the
    existing document:

      >>> top.name = 'top'
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1b16537a08e2d1a000001'), 'mongopersist_test')
      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      [{u'_id': ObjectId('4eb1b17937a08e2d29000001'), u'name': u'top'}]

    """

def doctest_ObjectWriter_store_with_mongo_store_type():
    """ObjectWriter: store(): _p_mongo_store_type = True

      >>> writer = serialize.ObjectWriter(dm)

      >>> top = Top()
      >>> top._p_mongo_store_type = True
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1b16537a08e2d1a000001'), 'mongopersist_test')
      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      [{u'_id': ObjectId('4eb1b27437a08e2d7d000003'),
        u'_py_persistent_type': u'mongopersist.tests.test_serialize.Top'}]
    """

def doctest_ObjectWriter_store_with_conflict_detection():
    """ObjectWriter: store(): conflict detection

    The writer supports the data manager's conflict detection by storing a
    serial number, which is effectively the version of the object. The data
    manager can then use the serial to detect whether a competing transaction
    has written to the document.

      >>> dm.detect_conflicts = True
      >>> writer = serialize.ObjectWriter(dm)

      >>> top = Top()
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1b16537a08e2d1a000001'), 'mongopersist_test')
      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      [{u'_id': ObjectId('4eb1b31137a08e2d9d000003'), u'_py_serial': 1}]
    """

def doctest_ObjectWriter_store_with_new_object_references():
    """ObjectWriter: store(): new object references

    When two new objects reference each other, extracting the full state would
    cause infinite recursion errors. The code protects against that by
    optionally only creating an initial empty reference document.

      >>> writer = serialize.ObjectWriter(dm)

      >>> top = Top()
      >>> top.foo = Foo()
      >>> top.foo.top = top
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1b16537a08e2d1a000001'), 'mongopersist_test')
      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      [{u'_id': ObjectId('4eb1b3d337a08e2de7000009'),
        u'foo': DBRef(u'Foo', ObjectId('4eb1b3d337a08e2de7000008'),
                      u'mongopersist_test')}]
    """

def doctest_ObjectReader_simple_resolve():
    """ObjectReader: simple_resolve()

    This methods simply resolves a Python path to the represented object.

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.simple_resolve('mongopersist.tests.test_serialize.Top')
      <class 'mongopersist.tests.test_serialize.Top'>
    """

def doctest_ObjectReader_resolve_simple():
    """ObjectReader: resolve(): simple

    This methods resolves a collection name to its class. The collection name
    can be either any arbitrary string or a Python path.

      >>> reader = serialize.ObjectReader(dm)
      >>> ref = dbref.DBRef('mongopersist.tests.test_serialize.Top',
      ...                   '4eb1b3d337a08e2de7000100')
      >>> reader.resolve(ref)
      <class 'mongopersist.tests.test_serialize.Top'>
    """

def doctest_ObjectReader_resolve_lookup():
    """ObjectReader: resolve(): lookup

    If Python path resolution fails, we try to lookup the path from the
    collection mapping collection names to Python paths.

      >>> reader = serialize.ObjectReader(dm)
      >>> ref = dbref.DBRef('Top', '4eb1b3d337a08e2de7000100', DBNAME)
      >>> reader.resolve(ref)
      Traceback (most recent call last):
      ...
      ImportError: DBRef('Top', '4eb1b3d337a08e2de7000100', 'mongopersist_test')

    The lookup failed, because there is no map entry yet for the 'Top'
    collection. The easiest way to create one is with the object writer:

      >>> top = Top()
      >>> writer = serialize.ObjectWriter(dm)
      >>> writer.get_collection_name(top)
      ('mongopersist_test', 'Top')

      >>> reader.resolve(ref)
      <class 'mongopersist.tests.test_serialize.Top'>
    """

def doctest_ObjectReader_resolve_lookup_with_multiple_maps():
    """ObjectReader: resolve(): lookup with multiple maps entries

    When the collection name to Python path map has multiple entries, things
    are more interesting. In this case, we need to lookup the object, if it
    stores its persistent type otherwise we use the first map entry.

      >>> writer = serialize.ObjectWriter(dm)
      >>> top = Top()
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1e0f237a08e38dd000002'), 'mongopersist_test')
      >>> top2 = Top2()
      >>> writer.store(top2)
      DBRef('Top', ObjectId('4eb1e10437a08e38e8000004'), 'mongopersist_test')

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.resolve(top._p_oid)
      <class 'mongopersist.tests.test_serialize.Top'>
      >>> reader.resolve(top2._p_oid)
      <class 'mongopersist.tests.test_serialize.Top2'>

      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      [{u'_id': ObjectId('4eb1e13337a08e392d000002')},
       {u'_id': ObjectId('4eb1e13337a08e392d000004'),
        u'_py_persistent_type': u'mongopersist.tests.test_serialize.Top2'}]

    If the DBRef does not have an object id, then an import error is raised:

      >>> reader.resolve(dbref.DBRef('Top', None, 'mongopersist_test'))
      Traceback (most recent call last):
      ...
      ImportError: DBRef('Top', None, 'mongopersist_test')
    """

def doctest_ObjectReader_get_non_persistent_object_py_type():
    """ObjectReader: get_non_persistent_object(): _py_type

    The simplest case is a document with a _py_type:

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.get_non_persistent_object(
      ...    {'_py_type': 'mongopersist.tests.test_serialize.Simple'}, None)
      <mongopersist.tests.test_serialize.Simple object at 0x306f410>

    It is a little bit more interesting when there is some additional state:

      >>> simple = reader.get_non_persistent_object(
      ...    {u'_py_type': 'mongopersist.tests.test_serialize.Simple',
      ...     u'name': u'Here'},
      ...    None)
      >>> simple.name
      u'Here'
    """

def doctest_ObjectReader_get_non_persistent_object_py_persistent_type():
    """ObjectReader: get_non_persistent_object(): _py_persistent_type

    In this case the document has a _py_persistent_type attribute, which
    signals a persistent object living in its parent's document:

      >>> top = Top()

      >>> reader = serialize.ObjectReader(dm)
      >>> tier2 = reader.get_non_persistent_object(
      ...    {'_py_persistent_type': 'mongopersist.tests.test_serialize.Tier2',
      ...     'name': 'Number 2'},
      ...    top)
      >>> tier2
      <mongopersist.tests.test_serialize.Tier2 object at 0x306f410>

    We keep track of the containing object, so we can set _p_changed when this
    object changes.

      >>> tier2._p_mongo_doc_object
      <mongopersist.tests.test_serialize.Top object at 0x7fa30b534050>
      >>> tier2._p_jar
      <mongopersist.datamanager.MongoDataManager object at 0x7fc3cab375d0>
    """

def doctest_ObjectReader_get_non_persistent_object_py_factory():
    """ObjectReader: get_non_persistent_object(): _py_factory

    This is the case of last resort. Specify a factory and its arguments:

      >>> reader = serialize.ObjectReader(dm)
      >>> top = reader.get_non_persistent_object(
      ...    {'_py_factory': 'mongopersist.tests.test_serialize.create_top',
      ...     '_py_factory_args': ('TOP',)},
      ...    None)
      >>> top
      <mongopersist.tests.test_serialize.Top object at 0x306f410>
      >>> top.name
      'TOP'
    """

def doctest_ObjectReader_get_object_ObjectId():
    """ObjectReader: get_object(): ObjectId

    The object id is special and we simply conserve it:

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.get_object(
      ...     objectid.ObjectId('4e827608e13823598d000003'), None)
      ObjectId('4e827608e13823598d000003')
    """

def doctest_ObjectReader_get_object_binary():
    """ObjectReader: get_object(): binary data

    Binary data is just converted to a string:

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.get_object(binary.Binary('hello'), None)
      'hello'
    """

def doctest_ObjectReader_get_object_dbref():
    """ObjectReader: get_object(): DBRef

      >>> writer = serialize.ObjectWriter(dm)
      >>> top = Top()
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1e0f237a08e38dd000002'), 'mongopersist_test')

    Database references load the ghost state of the obejct they represent:

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.get_object(top._p_oid, None)
      <mongopersist.tests.test_serialize.Top object at 0x2801938>
    """

def doctest_ObjectReader_get_object_type_ref():
    """ObjectReader: get_object(): type reference

    Type references are resolved.

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.get_object(
      ...     {'_py_type': 'type',
      ...      'path': 'mongopersist.tests.test_serialize.Simple'},
      ...     None)
      <class 'mongopersist.tests.test_serialize.Simple'>
    """

def doctest_ObjectReader_get_object_instance():
    """ObjectReader: get_object(): instance

    Instances are completely loaded:

      >>> reader = serialize.ObjectReader(dm)
      >>> simple = reader.get_object(
      ...     {u'_py_type': 'mongopersist.tests.test_serialize.Simple',
      ...      u'name': u'easy'},
      ...     None)
      >>> simple
      <mongopersist.tests.test_serialize.Simple object at 0x2bcc950>
      >>> simple.name
      u'easy'
    """

def doctest_ObjectReader_get_object_sequence():
    """ObjectReader: get_object(): sequence

    Sequences become persistent lists with all obejcts deserialized.

      >>> reader = serialize.ObjectReader(dm)
      >>> reader.get_object([1, '2', 3.0], None)
      [1, '2', 3.0]
    """

def doctest_ObjectReader_get_object_mapping():
    """ObjectReader: get_object(): mapping

    Mappings become persistent dicts with all obejcts deserialized.

      >>> reader = serialize.ObjectReader(dm)
      >>> pprint.pprint(reader.get_object({'1': 1, '2': 2, '3': 3}, None))
      {'1': 1, '3': 3, '2': 2}

    Since Mongo does not allow for non-string keys, the state for a dict with
    non-string keys looks different:

      >>> pprint.pprint(reader.get_object(
      ...     {'dict_data': [(1, '1'), (2, '2'), (3, '3')]},
      ...     None))
      {1: '1', 2: '2', 3: '3'}
    """

def doctest_ObjectReader_get_ghost():
    """ObjectReader: get_ghost()

      >>> writer = serialize.ObjectWriter(dm)
      >>> top = Top()
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1e0f237a08e38dd000002'), 'mongopersist_test')

    The ghost object is a shell without any loaded object state:

      >>> reader = serialize.ObjectReader(dm)
      >>> gobj = reader.get_ghost(top._p_oid)
      >>> gobj._p_jar
      <mongopersist.datamanager.MongoDataManager object at 0x2720e50>
      >>> gobj._p_state
      0

    The second time we look up the object, it comes from cache:

      >>> gobj = reader.get_ghost(top._p_oid)
      >>> gobj._p_state
      0
    """

def doctest_ObjectReader_set_ghost_state():
    r"""ObjectReader: set_ghost_state()

      >>> dm.detect_conflicts = True

      >>> writer = serialize.ObjectWriter(dm)
      >>> top = Top()
      >>> top.name = 'top'
      >>> writer.store(top)
      DBRef('Top', ObjectId('4eb1e0f237a08e38dd000002'), 'mongopersist_test')

    The ghost object is a shell without any loaded object state:

      >>> reader = serialize.ObjectReader(dm)
      >>> gobj = reader.get_ghost(top._p_oid)
      >>> gobj._p_jar
      <mongopersist.datamanager.MongoDataManager object at 0x2720e50>
      >>> gobj._p_state
      0

    Now load the state:

      >>> reader.set_ghost_state(gobj)
      >>> gobj.name
      u'top'
      >>> gobj._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x01'
    """



def doctest_deserialize_persistent_references():
    """Deserialization o persistent references.

    The purpose of this test is to demonstrate the proper deserialization of
    persistent object references.

    Let's create a simple object hierarchy:

      >>> top = Top()
      >>> top.name = 'top'
      >>> top.foo = Foo()
      >>> top.foo.name = 'foo'

      >>> dm.root['top'] = top
      >>> commit()

    Let's check that the objects were properly serialized.

      >>> pprint.pprint(list(conn[DBNAME]['Top'].find()))
      [{u'_id': ObjectId('4e827608e13823598d000003'),
        u'foo': DBRef(u'Foo',
                      ObjectId('4e827608e13823598d000002'),
                      u'mongopersist_test'),
        u'name': u'top'}]
      >>> pprint.pprint(list(conn[DBNAME]['Foo'].find()))
      [{u'_id': ObjectId('4e8276c3e138235a2e000002'), u'name': u'foo'}]

    Now we access the objects objects again to see whether they got properly
    deserialized.

      >>> top2 = dm.root['top']
      >>> id(top2) == id(top)
      False
      >>> top2.name
      u'top'

      >>> id(top2.foo) == id(top.foo)
      False
      >>> top2.foo
      <mongopersist.tests.test_serialize.Foo object at 0x7fb1a0c0b668>
      >>> top2.foo.name
      u'foo'
    """


def test_suite():
    return doctest.DocTestSuite(
        setUp=testing.setUp, tearDown=testing.tearDown,
        checker=testing.checker,
        optionflags=testing.OPTIONFLAGS)
