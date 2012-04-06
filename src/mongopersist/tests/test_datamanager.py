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
"""Mongo  Tests"""
import doctest
import persistent
import transaction
from pymongo import dbref, objectid

from mongopersist import conflict, interfaces, testing, datamanager

class Foo(persistent.Persistent):
    def __init__(self, name=None):
        self.name = name

class Bar(persistent.Persistent):
    _p_mongo_sub_object = True

def doctest_Root():
    r"""Root: General Test

    This class represents the root(s) of the object tree. All roots are stored
    in a specified collection. Since the rooted object needs to immediately
    provide a data manager (jar), the operations on the DB root are not art of
    the transaction mechanism.

      >>> root = datamanager.Root(dm, DBNAME, 'proot')

    Initially the root is empty:

      >>> root.keys()
      []

    Let's now add an item:

      >>> foo = Foo()
      >>> root['foo'] = foo
      >>> root.keys()
      [u'foo']
      >>> root['foo'] == foo
      True

    Root objects can be overridden:

      >>> foo2 = Foo()
      >>> root['foo'] = foo2
      >>> root.keys()
      [u'foo']
      >>> root['foo'] == foo
      False

    And of course we can delete an item:

      >>> del root['foo']
      >>> root.keys()
      []
    """

def doctest_MongoDataManager_get_collection():
    r"""MongoDataManager: get_collection(db_name, coll_name)

    Get the collection given the DB and collection name.

      >>> foo = Foo('1')
      >>> foo_ref = dm.insert(foo)
      >>> dm.reset()

      >>> coll = dm.get_collection(
      ...     DBNAME, 'mongopersist.tests.test_datamanager.Foo')

    We are returning a collection wrapper instead, so that we can flush the
    data before any method involving a query.

      >>> coll
      <mongopersist.datamanager.CollectionWrapper object at 0x19e47d0>
      >>> coll.collection
      Collection(Database(Connection('localhost', 27017), u'mongopersist_test'),
                 u'mongopersist.tests.test_datamanager.Foo')

    Let's now make a query:

      >>> tuple(coll.find())
      ({u'_id': ObjectId('4f5c1bf537a08e2ea6000000'), u'name': u'1'},)
    """

def doctest_MongoDataManager_get_collection_from_object():
    r"""MongoDataManager: get_collection_from_object(obj)

    Get the collection for an object.

      >>> foo = Foo('1')
      >>> foo_ref = dm.insert(foo)
      >>> dm.reset()

      >>> coll = dm.get_collection_from_object(foo)

    We are returning a collection wrapper instead, so that we can flush the
    data before any method involving a query.

      >>> coll
      <mongopersist.datamanager.CollectionWrapper object at 0x19e47d0>

      >>> coll.collection
      Collection(Database(Connection('localhost', 27017), u'mongopersist_test'),
                 u'mongopersist.tests.test_datamanager.Foo')

    Let's make sure that modifying attributes is done on the original
    collection:

      >>> coll.foo = 1
      >>> coll.collection.foo
      1
      >>> coll.foo
      1
      >>> del coll.foo

    Let's now try the real functionality behind the wrapper. So we are in a
    transaction and modify an object:

      >>> foo_new = dm.load(foo_ref)
      >>> foo_new.name = '2'

    If we do not use the wrapper, the change is not visible:

      >>> tuple(dm._get_collection_from_object(foo_new).find())
      ({u'_id': ObjectId('4f5c1bf537a08e2ea6000000'), u'name': u'1'},)

    But if we use the wrapper, the change gets flushed first:

      >>> tuple(dm.get_collection_from_object(foo_new).find())
      ({u'_id': ObjectId('4f5c1bf537a08e2ea6000000'), u'name': u'2'},)

    Of course, aborting the transaction gets us back to the original state:

      >>> dm.abort(transaction.get())
      >>> tuple(dm._get_collection_from_object(foo_new).find())
      ({u'_id': ObjectId('4f5c1bf537a08e2ea6000000'), u'name': u'1'},)
    """

def doctest_MongoDataManager_object_dump_load_reset():
    r"""MongoDataManager: dump(), load(), reset()

    The Mongo Data Manager is a persistent data manager that manages object
    states in a Mongo database accross Python transactions.

    There are several arguments to create the data manager, but only the
    pymongo connection is required:

      >>> dm = datamanager.MongoDataManager(
      ...     conn,
      ...     default_database = DBNAME,
      ...     root_database = DBNAME,
      ...     root_collection = 'proot',
      ...     name_map_collection = 'coll_pypath_map')

    There are two convenience methods that let you serialize and de-serialize
    objects explicitly:

      >>> foo = Foo()
      >>> dm.dump(foo)
      DBRef('mongopersist.tests.test_datamanager.Foo',
            ObjectId('4eb2eb7437a08e0156000000'),
            'mongopersist_test')

    When the object is modified, ``dump()`` will remove it from the list of
    registered objects.

      >>> foo.name = 'Foo'
      >>> foo._p_changed
      True
      >>> dm._registered_objects
      [<mongopersist.tests.test_datamanager.Foo object at 0x2fe1f50>]

      >>> foo_ref = dm.dump(foo)

      >>> foo._p_changed
      False
      >>> dm._registered_objects
      []

    Let's now reset the data manager, so we do not hit a cache while loading
    the object again:

      >>> dm.reset()

    We can now load the object:

      >>> foo2 = dm.load(foo._p_oid)
      >>> foo == foo2
      False
      >>> foo._p_oid = foo2._p_oid
    """

def doctest_MongoDataManager_dump_only_on_real_change():
    r"""MongoDataManager: dump(): dump on real change only.

    The data manager only writes data when we actually have a difference in
    state.

    We have to use a serial conflict handler, otherwise it is hard to check
    whether data was written.

      >>> dm.conflict_handler = conflict.SimpleSerialConflictHandler(dm)

    Let's now add an object:

      >>> foo = Foo('foo')
      >>> foo_ref = dm.insert(foo)
      >>> dm.tpc_finish(None)

      >>> coll = dm._get_collection_from_object(foo)
      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'_py_serial': 1, u'name': u'foo'}

    So the original state is in. Let's now modify an object:

      >>> foo = dm.load(foo_ref)
      >>> foo.name = 'Foo'
      >>> foo._p_changed
      True
      >>> dm.tpc_finish(None)

      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'_py_serial': 2, u'name': u'Foo'}

    If we now modify the object again, but write the same value, the state
    should not be written to Mongo.

      >>> foo = dm.load(foo_ref)
      >>> foo.name = 'Foo'
      >>> foo._p_changed
      True
      >>> dm.tpc_finish(None)

      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'_py_serial': 2, u'name': u'Foo'}

    Let's make sure everything also works when we flush the transaction in the
    middle.

      >>> foo = dm.load(foo_ref)
      >>> foo.name = 'fuh'
      >>> dm.flush()
      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'_py_serial': 3, u'name': u'fuh'}

      >>> foo._p_changed
      False
      >>> foo.name = 'fuh'
      >>> foo._p_changed
      True

      >>> dm.tpc_finish(None)
      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'_py_serial': 3, u'name': u'fuh'}
    """

def doctest_MongoDataManager_dump_only_on_real_change_no_py_serial():
    r"""MongoDataManager: dump(): dump on real change only.

    Quirk: some objects might not have _py_serial in their state

    The data manager only writes data when we actually have a difference in
    state.

    We have to use a serial conflict handler, otherwise it is hard to check
    whether data was written.

      >>> dm.conflict_handler = conflict.SimpleSerialConflictHandler(dm)

    Let's now add an object:

      >>> foo = Foo('foo')
      >>> foo_ref = dm.insert(foo)
      >>> dm.tpc_finish(None)

      >>> coll = dm._get_collection_from_object(foo)
      >>> state = coll.find_one({})
      >>> state
      {u'_id': ObjectId('...'), u'_py_serial': 1, u'name': u'foo'}

      >>> del state['_py_serial']
      >>> coll.save(state)
      ObjectId('...')

      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'name': u'foo'}

    So the original state is in. Let's now modify an object:

      >>> foo = dm.load(foo_ref)
      >>> foo.name = 'Foo'
      >>> foo._p_changed
      True
      >>> dm.tpc_finish(None)

    _py_serial gets added silently, without an exception

      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'_py_serial': 1, u'name': u'Foo'}

    """

def doctest_MongoDataManager_flush():
    r"""MongoDataManager: flush()

    This method writes all registered objects to Mongo. It can be used at any
    time during the transaction when a dump is necessary, but is also used at
    the end of the transaction to dump all remaining objects.

    We also want to test the effects of conflict detection:

      >>> dm.conflict_handler = conflict.SimpleSerialConflictHandler(dm)

    Let's now add an object to the database and reset the manager like it is
    done at the end of a transaction:

      >>> foo = Foo('foo')
      >>> foo_ref = dm.dump(foo)
      >>> dm.reset()

    Let's now load the object again and make a modification:

      >>> foo_new = dm.load(foo._p_oid)
      >>> foo_new.name = 'Foo'

    The object is now registered with the data manager:

      >>> dm._registered_objects
      [<mongopersist.tests.test_datamanager.Foo object at 0x2f7b9b0>]
      >>> foo_new._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x01'

    Let's now flush the registered objects:

      >>> dm.flush()

    There are several side effects that should be observed:

    * During a given transaction, we guarantee that the user will always receive
      the same Python object. This requires that flush does not reset the object
      cache.

        >>> id(dm.load(foo._p_oid)) == id(foo_new)
        True

    * The ``_p_serial`` is increased by one.

        >>> foo_new._p_serial
        '\x00\x00\x00\x00\x00\x00\x00\x02'

    * The object is removed from the registered objects and the ``_p_changed``
      flag is set to ``False``.

        >>> dm._registered_objects
        []
        >>> foo_new._p_changed
        False

    * Before flushing, potential conflicts must be detected as it is done before
      committing a transaction.

        >>> foo_new._p_serial = '\x00\x00\x00\x00\x00\x00\x00\x01'
        >>> foo_new.name = 'Foo'
        >>> dm.flush()
        Traceback (most recent call last):
        ...
        ConflictError: database conflict error
            (oid DBRef('mongopersist.tests.test_datamanager.Foo',
                       ObjectId('4e7ddf12e138237403000000'),
                       'mongopersist_test'),
             class Foo,
             orig serial 1, cur serial 2, new serial 2)
    """

def doctest_MongoDataManager_insert():
    r"""MongoDataManager: insert(obj)

    This method inserts an object into the database.

      >>> foo = Foo('foo')
      >>> foo_ref = dm.insert(foo)

    After insertion, the original is not changed:

      >>> foo._p_changed
      False

    It is also added to the list of inserted objects:

      >>> dm._inserted_objects
      [<mongopersist.tests.test_datamanager.Foo object at 0x18d41b8>]

    Let's make sure it is really in Mongo:

      >>> dm.reset()
      >>> foo_new = dm.load(foo_ref)
      >>> foo_new
      <mongopersist.tests.test_datamanager.Foo object at 0x27cade8>

    Notice, that we cannot insert the object again:

      >>> dm.insert(foo_new)
      Traceback (most recent call last):
      ...
      ValueError: ('Object has already an OID.',
                   <mongopersist.tests.test_datamanager.Foo object at 0x1fecde8>)

    Finally, registering a new object will not trigger an insert, but only
    schedule the object for writing. This is done, since sometimes objects are
    registered when we only want to store a stub since we otherwise end up in
    endless recursion loops.

      >>> foo2 = Foo('Foo 2')
      >>> dm.register(foo2)

      >>> dm._registered_objects
      [<mongopersist.tests.test_datamanager.Foo object at 0x3087b18>]

    But storing works as expected (flush is implicit before find):

      >>> tuple(dm.get_collection_from_object(foo2).find())
      ({u'_id': ObjectId('4f5c443837a08e37bf000000'), u'name': u'foo'},
       {u'_id': ObjectId('4f5c443837a08e37bf000001'), u'name': u'Foo 2'})
    """

def doctest_MongoDataManager_insert_conflict_detection():
    r"""MongoDataManager: insert(obj): Conflict Detection.

    This test ensures that if the datamanager has conflict detection turned
    on, all the needed helper fields are written.

      >>> dm.conflict_handler = conflict.SimpleSerialConflictHandler(dm)
      >>> foo = Foo('foo')
      >>> foo_ref = dm.insert(foo)

    Let's check that all the fields are there:

      >>> coll = dm.get_collection_from_object(foo)
      >>> coll.find_one({})
      {u'_id': ObjectId('4f74837237a08e186f000000'), u'_py_serial': 1,
       u'name': u'foo'}
    """


def doctest_MongoDataManager_remove():
    r"""MongoDataManager: remove(obj)

    This method removes an object from the database.

      >>> foo = Foo('foo')
      >>> foo_ref = dm.insert(foo)
      >>> dm.reset()

    Let's now load the object and remove it.

      >>> foo_new = dm.load(foo_ref)
      >>> dm.remove(foo_new)

    The object is removed from the collection immediately:

      >>> tuple(dm._get_collection_from_object(foo_ref).find())
      ()

    Also, the object is added to the list of removed objects:

      >>> dm._removed_objects
      [<mongopersist.tests.test_datamanager.Foo object at 0x1693140>]

    Note that you cannot remove objects that are not in the database:

      >>> dm.remove(Foo('Foo 2'))
      Traceback (most recent call last):
      ValueError: ('Object does not have OID.',
                   <mongopersist.tests.test_datamanager.Foo object at 0x1982ed8>)

    There is an edge case, if the object is inserted and removed in the same
    transaction:

      >>> dm.reset()
      >>> foo3 = Foo('Foo 3')
      >>> foo3_ref = dm.insert(foo3)
      >>> dm.remove(foo3)

    In this case, the object removed from Mongo and from the inserted object
    list and never added to the removed object list.

      >>> dm._inserted_objects
      []
      >>> dm._removed_objects
      []

    """

def doctest_MongoDataManager_setstate():
    r"""MongoDataManager: setstate()

    This method loads and sets the state of an object and joins the
    transaction.

      >>> foo = Foo(u'foo')
      >>> ref = dm.dump(foo)

      >>> dm.reset()
      >>> dm._needs_to_join
      True

      >>> foo2 = Foo()
      >>> foo2._p_oid = ref
      >>> dm.setstate(foo2)
      >>> foo2.name
      u'foo'

      >>> dm._needs_to_join
      False
    """

def doctest_MongoDataManager_oldstate():
    r"""MongoDataManager: oldstate()

    Loads the state of an object for a given transaction. Since we are not
    supporting history, this always raises a key error as documented.

      >>> foo = Foo(u'foo')
      >>> dm.oldstate(foo, '0')
      Traceback (most recent call last):
      ...
      KeyError: '0'
    """

def doctest_MongoDataManager_register():
    r"""MongoDataManager: register()

    Registers an object to be stored.

      >>> dm._needs_to_join
      True
      >>> len(dm._registered_objects)
      0

      >>> foo = Foo(u'foo')
      >>> dm.register(foo)

      >>> dm._needs_to_join
      False
      >>> len(dm._registered_objects)
      1

   But there are no duplicates:

      >>> dm.register(foo)
      >>> len(dm._registered_objects)
      1
    """

def doctest_MongoDataManager_abort():
    r"""MongoDataManager: abort()

    Aborts a transaction, which clears all object and transaction registrations:

      >>> dm._registered_objects = [Foo()]
      >>> dm._needs_to_join = False

      >>> dm.abort(transaction.get())

      >>> dm._needs_to_join
      True
      >>> len(dm._registered_objects)
      0

    Let's now create a more interesting case with a transaction that inserted,
    removed and changed objects.

    First let's create an initial state:

      >>> dm.reset()
      >>> foo_ref = dm.insert(Foo('one'))
      >>> foo2_ref = dm.insert(Foo('two'))
      >>> dm.reset()

      >>> coll = dm._get_collection_from_object(Foo())
      >>> tuple(coll.find({}))
      ({u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'one'},
       {u'_id': ObjectId('4f5c114f37a08e2cac000001'), u'name': u'two'})

    Now, in a second transaction we modify the state of objects in all three
    ways:

      >>> foo = dm.load(foo_ref)
      >>> foo.name = '1'
      >>> dm._registered_objects
      [<mongopersist.tests.test_datamanager.Foo object at 0x187b1b8>]

      >>> foo2 = dm.load(foo2_ref)
      >>> dm.remove(foo2)
      >>> dm._removed_objects
      [<mongopersist.tests.test_datamanager.Foo object at 0x1e5c140>]

      >>> foo3_ref = dm.insert(Foo('three'))

      >>> dm.flush()
      >>> tuple(coll.find({}))
      ({u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'1'},
       {u'_id': ObjectId('4f5c114f37a08e2cac000002'), u'name': u'three'})

    Let's now abort the transaction and everything should be back to what it
    was before:

      >>> dm.abort(transaction.get())
      >>> tuple(coll.find({}))
      ({u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'one'},
       {u'_id': ObjectId('4f5c114f37a08e2cac000001'), u'name': u'two'})
    """

def doctest_MongoDataManager_abort_modified_only():
    r"""MongoDataManager: abort(): Only reset changed objects.

    We want to make sure that we only reset modified objects, not all objects
    that have been loaded. The ratio from reads to writes is very high, so
    unexpected behavior with other transactions is decreased by that ratio.

    First let's create an initial state:

      >>> dm.reset()
      >>> foo1_ref = dm.insert(Foo('one'))
      >>> foo2_ref = dm.insert(Foo('two'))
      >>> foo3_ref = dm.insert(Foo('three'))
      >>> dm.reset()

      >>> coll = dm._get_collection_from_object(Foo())
      >>> tuple(coll.find({}))
      ({u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'one'},
       {u'_id': ObjectId('4f5c114f37a08e2cac000001'), u'name': u'two'},
       {u'_id': ObjectId('4f5c114f37a08e2cac000002'), u'name': u'three'})

    1. Transaction A loads all objects:

        >>> foo1_A = dm.load(foo1_ref)
        >>> foo1_A.name
        u'one'
        >>> foo2_A = dm.load(foo2_ref)
        >>> foo2_A.name
        u'two'
        >>> foo3_A = dm.load(foo3_ref)
        >>> foo3_A.name
        u'three'

        >>> sorted([ref.id for ref in dm._original_states.keys()])
        [ObjectId('4f746d0b37a08e1013000000'),
         ObjectId('4f746d0b37a08e1013000001'),
         ObjectId('4f746d0b37a08e1013000002')]

    2. Transaction B comes along and modifies Foo 3's data and commits:

        >>> dm_B = datamanager.MongoDataManager(
        ...     conn, default_database=DBNAME, root_database=DBNAME)

        >>> foo3_B = dm_B.load(foo3_ref)
        >>> foo3_B.name = '3'
        >>> dm_B.tpc_finish(None)

        >>> tuple(coll.find({}))
        ({u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'one'},
         {u'_id': ObjectId('4f5c114f37a08e2cac000001'), u'name': u'two'},
         {u'_id': ObjectId('4f5c114f37a08e2cac000002'), u'name': u'3'})

    3. Transaction A modifies Foo 1 and the data is flushed:

        >>> foo1_A.name = '1'
        >>> dm.flush()

        >>> tuple(coll.find({}))
        ({u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'1'},
         {u'_id': ObjectId('4f5c114f37a08e2cac000001'), u'name': u'two'},
         {u'_id': ObjectId('4f5c114f37a08e2cac000002'), u'name': u'3'})

    4. If transcation A is later aborted, only objects modified within the
       transaction get reset to their original state (and not all loaded ones:

       >>> dm.abort(None)

        >>> tuple(coll.find({}))
        ({u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'one'},
         {u'_id': ObjectId('4f5c114f37a08e2cac000001'), u'name': u'two'},
         {u'_id': ObjectId('4f5c114f37a08e2cac000002'), u'name': u'3'})
    """

def doctest_MongoDataManager_abort_conflict_detection():
    r"""MongoDataManager: abort(): Conflict detections while aborting.

    When a transaction is aborting, we are usually resetting the state of the
    modified objects. What happens, however, when the document was updated
    since the last flush?

    The implemented policy now does not reset the state in this case.

    First let's create an initial state:

      >>> dm.conflict_handler = conflict.SimpleSerialConflictHandler(dm)
      >>> dm.reset()
      >>> foo_ref = dm.insert(Foo('one'))
      >>> dm.reset()
      >>> coll = dm._get_collection_from_object(Foo())

    1. Transaction A loads the object and modifies it:

       >>> foo_A = dm.load(foo_ref)
       >>> foo_A.name = u'1'
       >>> coll.find_one({})
       {u'_id': ObjectId('4e7ddf12e138237403000000'),
        u'_py_serial': 1, u'name': u'one'}

    2. Transaction B comes along and modifies the object as well and commits:

       >>> dm_B = datamanager.MongoDataManager(
       ...     conn,
       ...     default_database=DBNAME, root_database=DBNAME,
       ...     conflict_handler_factory=conflict.SimpleSerialConflictHandler)

       >>> foo_B = dm_B.load(foo_ref)
       >>> foo_B.name = 'Eins'
       >>> dm_B.tpc_finish(None)
       >>> coll.find_one({})
       {u'_id': ObjectId('4e7ddf12e138237403000000'), u'_py_serial': 2,
        u'name': u'Eins'}

    3. If transcation A is later aborted, it does not reset the state, since
       it changed:

       >>> dm.abort(None)
       >>> coll.find_one({})
       {u'_id': ObjectId('4e7ddf12e138237403000000'), u'_py_serial': 2,
        u'name': u'Eins'}

    """

def doctest_MongoDataManager_tpc_begin():
    r"""MongoDataManager: tpc_begin()

    This is a non-op for the mongo data manager.

      >>> dm.tpc_begin(transaction.get())
    """

def doctest_MongoDataManager_tpc_vote():
    r"""MongoDataManager: tpc_vote()

    This is a non-op for the mongo data manager.

      >>> dm.tpc_vote(transaction.get())
    """

def doctest_MongoDataManager_tpc_finish():
    r"""MongoDataManager: tpc_finish()

    This method finishes the two-phase commit. So let's store a simple object:

      >>> foo = Foo()
      >>> dm.conflict_handler = conflict.SimpleSerialConflictHandler(dm)
      >>> dm._registered_objects = [foo]
      >>> dm.tpc_finish(transaction.get())
      >>> foo._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x01'

    Note that objects cannot be stored twice in the same transation:

      >>> dm.reset()
      >>> dm._registered_objects = [foo, foo]
      >>> dm.tpc_finish(transaction.get())
      >>> foo._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x02'

    Also, when a persistent sub-object is stored that does not want its own
    document, then its parent is stored instead, still avoiding dual storage.

      >>> dm.reset()
      >>> foo2 = dm.load(foo._p_oid)
      >>> foo2.bar = Bar()

      >>> dm.tpc_finish(transaction.get())
      >>> foo2._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x03'

      >>> dm.reset()
      >>> foo3 = dm.load(foo._p_oid)
      >>> dm._registered_objects = [foo3.bar, foo3]
      >>> dm.tpc_finish(transaction.get())
      >>> foo3._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x04'

    """

def doctest_MongoDataManager_tpc_abort():
    r"""MongoDataManager: tpc_abort()

    Aborts a two-phase commit. This is simply the same as the regular abort.

      >>> dm._registered_objects = [Foo()]
      >>> dm._needs_to_join = False

      >>> dm.tpc_abort(transaction.get())

      >>> dm._needs_to_join
      True
      >>> len(dm._registered_objects)
      0
    """

def doctest_MongoDataManager_sortKey():
    r"""MongoDataManager: sortKey()

    The data manager's sort key is trivial.

      >>> dm.sortKey()
      ('MongoDataManager', 0)
    """

def doctest_process_spec():
    r"""process_spec(): General test

    A simple helper function that returns the spec itself if no
    ``IMongoSpecProcessor`` adapter is registered. If a processor is found it
    is applied. The spec processor can be used for:

    * Additional logging.

    * Modifying the spec, for example providing additional parameters.

    Let's now call the function:

      >>> from zope.testing.cleanup import CleanUp as PlacelessSetup
      >>> PlacelessSetup().setUp()

      >>> datamanager.process_spec('a_collection', {'life': 42})
      {'life': 42}

    Now let's register an adapter

      >>> class Processor(object):
      ...     def __init__(self, context):
      ...         pass
      ...     def process(self, collection, spec):
      ...         print 'passed in:', collection, spec
      ...         return {'life': 24}

      >>> import zope.interface
      >>> from zope.component import provideAdapter
      >>> provideAdapter(
      ...     Processor,
      ...     (zope.interface.Interface,), interfaces.IMongoSpecProcessor)

    And see what happens on calling ``process_spec()``:

      >>> datamanager.process_spec('a_collection', {'life': 42})
      passed in: a_collection {'life': 42}
      {'life': 24}

    We get the processed spec in return.

      >>> PlacelessSetup().tearDown()

    """

def doctest_FlushDecorator_basic():
    r"""class FlushDecorator: basic functionality

    The FlushDecorator class can be used to ensure that data is flushed before
    a given function is called. Let's create an object and modify it:

      >>> foo = Foo('foo')
      >>> foo_ref = dm.dump(foo)
      >>> dm.reset()
      >>> foo_new = dm.load(foo._p_oid)
      >>> foo_new.name = 'Foo'

    The database is not immediately updated:

      >>> coll = conn[DBNAME]['mongopersist.tests.test_datamanager.Foo']
      >>> list(coll.find())
      [{u'_id': ObjectId('4e7ddf12e138237403000000'), u'name': u'foo'}]


    But when I use the decorator, all outstanding changes are updated at
    first:

      >>> flush_find = datamanager.FlushDecorator(dm, coll.find)
      >>> list(flush_find())
      [{u'_id': ObjectId('4e7ddf12e138237403000000'), u'name': u'Foo'}]

    """

def doctest_ProcessSpecDecorator_basic():
    r"""class ProcessSpecDecorator: basic

    The ``ProcessSpecDecorator`` decorator processes the spec before passing
    it to the function. Currently the following collection methods are
    supported: ``find_one()``, ``find()``, ``find_and_modify``.

    Now let's register an adapter

      >>> from zope.testing.cleanup import CleanUp as PlacelessSetup
      >>> PlacelessSetup().setUp()

      >>> class Processor(object):
      ...     def __init__(self, context):
      ...         pass
      ...     def process(self, collection, spec):
      ...         print 'passed in:', spec
      ...         return spec

      >>> import zope.interface
      >>> from zope.component import provideAdapter
      >>> provideAdapter(
      ...     Processor,
      ...     (zope.interface.Interface,), interfaces.IMongoSpecProcessor)

    Let's now create the decorator:

      >>> coll = conn[DBNAME]['mongopersist.tests.test_datamanager.Foo']
      >>> process_find = datamanager.ProcessSpecDecorator(coll, coll.find)
      >>> list(process_find({'life': 42}))
      passed in: {'life': 42}
      []

    Keyword arguments are also supported:

      >>> process_find = datamanager.ProcessSpecDecorator(coll, coll.find)
      >>> list(process_find(spec={'life': 42}))
      passed in: {'life': 42}
      []

      >>> process_find_one = datamanager.ProcessSpecDecorator(
      ...     coll, coll.find_one)
      >>> process_find_one(spec_or_id={'life': 42})
      passed in: {'life': 42}

      >>> process_find_one = datamanager.ProcessSpecDecorator(
      ...     coll, coll.find_one)
      >>> process_find_one(query={'life': 42})
      passed in: {'life': 42}

    We get the processed spec in return.

      >>> PlacelessSetup().tearDown()
    """


def doctest_LoggingDecorator_basic():
    r"""class LoggingDecorator: basic

    The ``LoggingDecorator`` decorator will log the name, arguments ans even
    current stack of a function call. Let's stub the logger:

      >>> orig_log_debug = datamanager.COLLECTION_LOG.debug
      >>> def fake_debug(msg, *args):
      ...     print msg % args
      >>> datamanager.COLLECTION_LOG.debug = fake_debug

    Let's create the decorator:

      >>> coll = conn[DBNAME]['mongopersist.tests.test_datamanager.Foo']
      >>> logging_find = datamanager.LoggingDecorator(coll, coll.find)
      >>> list(logging_find({'life': 42}))
      collection: mongopersist_test.mongopersist.tests.test_datamanager.Foo find,
       TXN:('... - ',),
       args:({'life': 42},),
       kwargs:{},
       tb:
          ...
          list(logging_find({'life': 42}))
      <BLANKLINE>
      []

    Keyword arguments are also supported:

      >>> list(logging_find(spec={'life': 42}))
      collection: mongopersist_test.mongopersist.tests.test_datamanager.Foo find,
       TXN:('... - ',),
       args:(),
       kwargs:{'spec': {'life': 42}},
       tb:
          ...
          list(logging_find(spec={'life': 42}))
      <BLANKLINE>
      []

    Tracebacks can also be turned off:

      >>> logging_find.ADD_TB = False
      >>> list(logging_find({'life': 42}))
      collection: mongopersist_test.mongopersist.tests.test_datamanager.Foo find,
       TXN:('... - ',),
       args:({'life': 42},),
       kwargs:{},
       tb:
        <omitted>
      []
    """

def test_suite():
    return doctest.DocTestSuite(
        setUp=testing.setUp, tearDown=testing.tearDown,
        checker=testing.checker,
        optionflags=testing.OPTIONFLAGS)
