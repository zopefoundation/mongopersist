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

from mongopersist import conflict, datamanager, interfaces, testing

class Foo(persistent.Persistent):
    def __init__(self, name=None):
        self.name = name
    def __repr__(self):
        return '<%s %r>' %(self.__class__.__name__, self.name)

class MergerList(persistent.Persistent):
    def __init__(self, list=None):
        self.list = list
    def __repr__(self):
        return '<%s %r>' %(self.__class__.__name__, self.list)
    def _p_resolveConflict(self, orig, cur, new):
        merged = orig.copy()
        merged['list'] = sorted(list(set(cur['list']).union(set(new['list']))))
        return merged

def doctest_create_conflict_error():
    r"""create_conflict_error(): General Test

    Simple helper function to create a conflict error.

     >>> foo = Foo()

     >>> conflict.create_conflict_error(
     ...     foo, {'_py_serial': 1}, {'_py_serial': 2}, {'_py_serial': 3})
     ConflictError: database conflict error
         (oid None, class Foo, orig serial 1, cur serial 2, new serial 3)
    """

def doctest_NoCheckConflictHandler_basic():
    r"""class NoCheckConflictHandler: basic

    This conflict handler does absolutely nothing to resolve conflicts. It is
    the default conflict handler of the library.

      >>> handler = conflict.NoCheckConflictHandler(dm)

    Let's check the event methods:

      >>> obj = Foo('one')
      >>> state = {'name': 'one'}

      >>> handler.on_before_set_state(obj, state)
      >>> obj, state
      (<Foo 'one'>, {'name': 'one'})

      >>> handler.on_before_store(obj, state)
      >>> obj, state
      (<Foo 'one'>, {'name': 'one'})

      >>> handler.on_after_store(obj, state)
      >>> obj, state
      (<Foo 'one'>, {'name': 'one'})

      >>> handler.on_modified(obj)
      >>> obj, state
      (<Foo 'one'>, {'name': 'one'})

    There is a method that allows for comparing 2 states of a given
    object. The method is used to detect whether objects really changed.

      >>> handler.is_same(obj, {'name': 'one'}, {'name': 'one'})
      True
      >>> handler.is_same(obj, {'name': 'one'}, {'name': 'eins'})
      False

    Let's check the conflict checking methods:

      >>> handler.has_conflicts([obj])
      False
      >>> handler.check_conflicts([obj])
    """

def doctest_NoCheckConflictHandler_full():
    r"""class NoCheckConflictHandler: Full conflict test.

    This test demonstrates the conflict resolution behavior of the
    ``NoCheckConflictHandler`` conflict handler during a real session.

    First let's create an initial state:

      >>> dm.reset()
      >>> foo_ref = dm.insert(Foo('one'))
      >>> dm.reset()

      >>> coll = dm._get_collection_from_object(Foo())
      >>> coll.find_one({})
      {u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'one'}

    1. Transaction A loads the object:

        >>> foo_A = dm.load(foo_ref)
        >>> foo_A.name
        u'one'

    2. Transaction B comes along and modifies Foos data and commits:

        >>> dm_B = datamanager.MongoDataManager(
        ...     conn, default_database=DBNAME, root_database=DBNAME,
        ...     conflict_handler_factory=conflict.NoCheckConflictHandler)

        >>> foo_B = dm_B.load(foo_ref)
        >>> foo_B.name = 'eins'
        >>> dm_B.tpc_finish(None)

        >>> coll.find_one({})
        {u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'eins'}

    3. Transaction A modifies Foo and the data is flushed:

        >>> foo_A.name = '1'
        >>> dm.flush()

        >>> coll.find_one({})
        {u'_id': ObjectId('4f5c114f37a08e2cac000000'), u'name': u'1'}
    """

def doctest_SimpleSerialConflictHandler_basic():
    r"""class SimpleSerialConflictHandler: basic

    This conflict handler detects conflicts by comparing serial numbers and
    always raises a ``ConflictError`` error.

      >>> handler = conflict.SimpleSerialConflictHandler(dm)
      >>> obj = Foo('one')

    Before the object state is set, the serial is extracted from the state and
    set on the object:

      >>> state = {'name': 'one', '_py_serial': 5}
      >>> handler.on_before_set_state(obj, state)
      >>> obj._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x05'
      >>> state
      {'name': 'one'}

    Before the object state is stored in Mongo, we add the serial to the
    document by taking the current one and add 1 to it. Note that the object's
    serial is not changed yet, since storing the document might still be
    cancelled (for example by detecting that the DB state equals the new
    state):

      >>> state = {'name': 'one'}
      >>> handler.on_before_store(obj, state)
      >>> obj._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x05'
      >>> state
      {'_py_serial': 6, 'name': 'one'}

    After the document was stored, we can safely update the object as well.

      >>> handler.on_after_store(obj, state)
      >>> obj._p_serial
      '\x00\x00\x00\x00\x00\x00\x00\x06'
      >>> state
      {'_py_serial': 6, 'name': 'one'}

    The event handler on modification does not need to do anything:

      >>> handler.on_modified(obj)
      >>> obj
      <Foo 'one'>

    There is a method that allows for comparing 2 states of a given
    object. The method is used to detect whether objects really changed.

      >>> handler.is_same(
      ...     obj,
      ...     {'name': 'one', '_py_serial': 1},
      ...     {'name': 'one', '_py_serial': 2})
      True
      >>> handler.is_same(
      ...     obj,
      ...     {'name': 'one', '_py_serial': 1},
      ...     {'name': 'eins', '_py_serial': 2})
      False

    As you can see, the serial number is omitted from the comparison, because
    it does not represent part of the object state, but is state meta-data.

    Let's check the conflict checking methods now. Initially, there are no
    conflicts:

      >>> handler.has_conflicts([obj])
      False
      >>> handler.check_conflicts([obj])

    We can force a conflict by setting back the serial on the object:

      >>> db_ref = dm.insert(obj)
      >>> dm.reset()
      >>> obj._p_serial = conflict.p64(3)

      >>> handler.has_conflicts([obj])
      True
      >>> handler.check_conflicts([obj])
      Traceback (most recent call last):
      ...
      ConflictError: database conflict error ...
    """

def doctest_SimpleSerialConflictHandler_full():
    r"""class SimpleSerialConflictHandler: Full conflict test.

    This test demonstrates the conflict resolution behavior of the
    ``SimpleSerialConflictHandler`` conflict handler during a real session.

    First let's create an initial state:

      >>> dm.conflict_handler = conflict.SimpleSerialConflictHandler(dm)
      >>> dm.reset()
      >>> foo_ref = dm.insert(Foo('one'))
      >>> dm.reset()

      >>> coll = dm._get_collection_from_object(Foo())
      >>> coll.find_one({})
      {u'_id': ObjectId('...'), u'_py_serial': 1, u'name': u'one'}

    1. Transaction A loads the object:

        >>> foo_A = dm.load(foo_ref)
        >>> foo_A.name
        u'one'

    2. Transaction B comes along and modifies Foos data and commits:

        >>> dm_B = datamanager.MongoDataManager(
        ...     conn, default_database=DBNAME, root_database=DBNAME,
        ...     conflict_handler_factory=conflict.SimpleSerialConflictHandler)

        >>> foo_B = dm_B.load(foo_ref)
        >>> foo_B.name = 'eins'
        >>> dm_B.tpc_finish(None)

        >>> coll.find_one({})
        {u'_id': ObjectId('...'), u'_py_serial': 2, u'name': u'eins'}

    3. Transaction A modifies Foo and the data is flushed. At this point a
       conflict is detected and reported:

        >>> foo_A.name = '1'
        >>> dm.flush()
        Traceback (most recent call last):
        ...
        ConflictError: database conflict error
            (oid DBRef('mongopersist.tests.test_conflict.Foo',
                       ObjectId('4f74bf0237a08e3085000002'),
                       'mongopersist_test'),
             class Foo, orig serial 1, cur serial 2, new serial 2)
    """

def doctest_ResolvingSerialConflictHandler_basic():
    r"""class ResolvingSerialConflictHandler: basic

    This conflict handler detects conflicts by comparing serial numbers and
    allows objects to resolve conflicts by calling their
    ``_p_resolveConflict()`` method. Otherwise the handler is identical to the
    simple serial handler.

      >>> handler = conflict.ResolvingSerialConflictHandler(dm)
      >>> l1 = MergerList([1, 2, 3])
      >>> l1.list
      [1, 2, 3]

      >>> handler.resolve(
      ...     l1,
      ...     orig_doc = {'list': [1, 2, 3], '_id': 1, '_py_serial': 0},
      ...     cur_doc = {'list': [1, 2, 3, 4], '_id': 1, '_py_serial': 1},
      ...     new_doc = {'list': [1, 2, 3, 5], '_id': 1, '_py_serial': 0})
      True
      >>> l1.list
      [1, 2, 3, 4, 5]

    Resolving always fails, if there is no ``_p_resolveConflict()`` method:

      >>> foo = Foo('one')
      >>> handler.resolve(
      ...     foo,
      ...     orig_doc = {'name': 'one', '_id': 1, '_py_serial': 0},
      ...     cur_doc = {'name': 'eins', '_id': 1, '_py_serial': 1},
      ...     new_doc = {'name': '1',    '_id': 1, '_py_serial': 0})
      False

    """

def doctest_ResolvingSerialConflictHandler_full():
    r"""class ResolvingSerialConflictHandler: Full conflict test.

    This test demonstrates the conflict resolution behavior of the
    ``ResolvingSerialConflictHandler`` conflict handler during a real session.

    First let's create an initial state:

      >>> dm.conflict_handler = conflict.ResolvingSerialConflictHandler(dm)
      >>> dm.reset()
      >>> ml = MergerList([1, 2, 3])
      >>> ml_ref = dm.insert(ml)
      >>> dm.reset()

      >>> coll = dm._get_collection_from_object(ml)
      >>> coll.find_one({})
      {u'list': [1, 2, 3], u'_id': ObjectId('...'), u'_py_serial': 1}

    1. Transaction A loads the object:

        >>> ml_A = dm.load(ml_ref)
        >>> ml_A.list
        [1, 2, 3]

    2. Transaction B comes along, adds a new item to the list and commits:

        >>> dm_B = datamanager.MongoDataManager(
        ...     conn, default_database=DBNAME, root_database=DBNAME,
        ...     conflict_handler_factory=conflict.ResolvingSerialConflictHandler)

        >>> ml_B = dm_B.load(ml_ref)
        >>> ml_B.list.append(4)
        >>> dm_B.tpc_finish(None)

        >>> coll.find_one({})
        {u'list': [1, 2, 3, 4], u'_id': ObjectId('...'), u'_py_serial': 2}

    3. Transaction A adds also an item  and the data is flushed. At this point a
       conflict is detected, reported and resolved:

        >>> ml_A.list.append(5)
        >>> ml_A._p_changed = True
        >>> ml_A.list
        [1, 2, 3, 5]
        >>> dm.flush()
        >>> ml_A.list
        [1, 2, 3, 4, 5]
        >>> ml_A._p_serial
        '\x00\x00\x00\x00\x00\x00\x00\x03'

        >>> coll.find_one({})
        {u'list': [1, 2, 3, 4, 5], u'_id': ObjectId('...'), u'_py_serial': 3}
    """

def test_suite():
    return doctest.DocTestSuite(
        setUp=testing.setUp, tearDown=testing.tearDown,
        checker=testing.checker,
        optionflags=testing.OPTIONFLAGS)
