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
import pprint
import transaction
from pymongo import dbref, objectid

from mongopersist import testing, datamanager

class Foo(persistent.Persistent):
    def __init__(self, name=None):
        self.name = name

class Bar(persistent.Persistent):
    _p_mongo_sub_object = True

def doctest_create_conflict_error():
    r"""create_conflict_error(): General Test

    Simple helper function to create a conflict error.

     >>> foo = Foo()
     >>> foo._p_serial = '\x00\x00\x00\x00\x00\x00\x00\x01'

     >>> datamanager.create_conflict_error(foo, {'_py_serial': 3})
     ConflictError: database conflict error
                    (oid None, class Foo, start serial 1, current serial 3)
    """

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

def doctest_MongoDataManager_object_dump_load_reset():
    r"""MongoDataManager: dump(), load(), reset()

    The Mongo Data Manager is a persistent data manager that manages object
    states in a Mongo database accross Python transactions.

    There are several arguments to create the data manager, but only the
    pymongo connection is required:

      >>> dm = datamanager.MongoDataManager(
      ...     conn,
      ...     detect_conflicts=True,
      ...     default_database = DBNAME,
      ...     root_database = DBNAME,
      ...     root_collection = 'proot',
      ...     name_map_collection = 'coll_pypath_map',
      ...     conflict_error_factory = datamanager.create_conflict_error)

    There are two convenience methods that let you serialize and de-serialize
    objects explicitly:

      >>> foo = Foo()
      >>> dm.dump(foo)
      DBRef('mongopersist.tests.test_datamanager.Foo',
            ObjectId('4eb2eb7437a08e0156000000'),
            'mongopersist_test')

    Let's now reset the data manager, so we do not hit a cache while loading
    the object again:

      >>> dm.reset()

    We can now load the object:

      >>> foo2 = dm.load(foo._p_oid)
      >>> foo == foo2
      False
      >>> foo._p_oid = foo2._p_oid
    """

def doctest_MongoDataManager_set_state():
    r"""MongoDataManager: set_state()

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
    """

def doctest_MongoDataManager_commit():
    r"""MongoDataManager: commit()

    Contrary to what the name suggests, this is the commit called during the
    first phase of a two-phase commit. Thus, for all practically purposes,
    this method merely checks whether the commit would potentially fail.

    This means, if conflict detection is disabled, this method does nothing.

      >>> dm.detect_conflicts
      False
      >>> dm.commit(transaction.get())

    Let's now turn on conflict detection:

      >>> dm.detect_conflicts = True

    For new objects (not having an oid), it always passes:

      >>> dm.reset()
      >>> dm._registered_objects = [Foo()]
      >>> dm.commit(transaction.get())

    If the object has an oid, but is not found in the DB, we also just pass,
    because the object will be inserted.

      >>> foo = Foo()
      >>> foo._p_oid =  dbref.DBRef(
      ...     'mongopersist.tests.test_datamanager.Foo',
      ...     objectid.ObjectId('4eb2eb7437a08e0156000000'),
      ...     'mongopersist_test')

      >>> dm.reset()
      >>> dm._registered_objects = [foo]
      >>> dm.commit(transaction.get())

    Let's now store an object and make sure it does not conflict:

      >>> foo = Foo()
      >>> ref = dm.dump(foo)
      >>> ref
      DBRef('mongopersist.tests.test_datamanager.Foo',
            ObjectId('4eb3468037a08e1b74000000'),
            'mongopersist_test')

      >>> dm.reset()
      >>> dm._registered_objects = [foo]
      >>> dm.commit(transaction.get())

    Next, let's cause a conflict byt simulating a conflicting transaction:

      >>> dm.reset()
      >>> foo2 = dm.load(ref)
      >>> foo2.name = 'foo2'
      >>> transaction.commit()

      >>> dm.reset()
      >>> dm._registered_objects = [foo]
      >>> dm.commit(transaction.get())
      Traceback (most recent call last):
      ...
      ConflictError: database conflict error
          (oid DBRef('mongopersist.tests.test_datamanager.Foo',
                     ObjectId('4eb3499637a08e1c5a000000'),
                     'mongopersist_test'),
           class Foo, start serial 1, current serial 2)
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
      >>> dm.detect_conflicts = True
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

def test_suite():
    return doctest.DocTestSuite(
        setUp=testing.setUp, tearDown=testing.tearDown,
        checker=testing.checker,
        optionflags=testing.OPTIONFLAGS)
