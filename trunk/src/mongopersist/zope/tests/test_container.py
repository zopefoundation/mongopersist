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
"""Mongo Persistence Doc Tests"""
import doctest

import ZODB
import ZODB.DemoStorage
import persistent
import pymongo
import re
import transaction
import zope.component
import zope.interface
import zope.lifecycleevent
from pprint import pprint
from zope.app.testing import placelesssetup
from zope.container import contained, btree
from zope.testing import module, renormalizing

from mongopersist import datamanager, interfaces, serialize
from mongopersist.zope import container

class ApplicationRoot(container.SimpleMongoContainer):
    _p_mongo_collection = 'root'

    def __repr__(self):
        return '<ApplicationRoot>'

class SimplePerson(contained.Contained, persistent.Persistent):
    _p_mongo_collection = 'person'

    def __init__(self, name):
        self.name = name

    def __str__(self):
        return self.name

    def __repr__(self):
        return '<%s %s>' %(self.__class__.__name__, self)

class Person(container.MongoContained, SimplePerson):
    pass


def doctest_SimpleMongoContainer_basic():
    """SimpleMongoContainer: basic

      >>> cn = 'mongopersist.zope.container.SimpleMongoContainer'

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Let's add a container to the root:

      >>> dm.reset()
      >>> dm.root['c'] = container.SimpleMongoContainer()

      >>> db = dm._conn[DBNAME]
      >>> pprint(list(db[cn].find()))
      [{u'_id': ObjectId('4e7ea146e13823316f000000'), u'data': {}}]

    As you can see, the serialization is very clean. Next we add a person.

      >>> dm.root['c'][u'stephan'] = SimplePerson(u'Stephan')
      ContainerModifiedEvent: <...SimpleMongoContainer ...>
      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c'][u'stephan']
      <SimplePerson Stephan>

      >>> dm.root['c']['stephan'].__parent__
      <mongopersist.zope.container.SimpleMongoContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

    You can also access objects using the ``get()`` method of course:

      >>> stephan = dm.root['c'].get(u'stephan')
      >>> stephan.__parent__
      <mongopersist.zope.container.SimpleMongoContainer object at 0x7fec50f86500>
      >>> stephan.__name__
      u'stephan'

    Let's commit and access the data again:

      >>> transaction.commit()

      >>> pprint(list(db['person'].find()))
      [{u'__name__': u'stephan',
        u'__parent__':
            DBRef(u'mongopersist.zope.container.SimpleMongoContainer',
                  ObjectId('4e7ddf12e138237403000000'),
                  u'mongopersist_container_test'),
        u'_id': ObjectId('4e7ddf12e138237403000000'),
        u'name': u'Stephan'}]

      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c']['stephan'].__parent__
      <mongopersist.zope.container.SimpleMongoContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

      >>> dm.root['c'].items()
      [(u'stephan', <SimplePerson Stephan>)]

      >>> dm.root['c'].values()
      [<SimplePerson Stephan>]

    Now remove the item:

      >>> del dm.root['c']['stephan']
      ContainerModifiedEvent: <...SimpleMongoContainer ...>

    The changes are immediately visible.

      >>> dm.root['c'].keys()
      []
      >>> dm.root['c']['stephan']
      Traceback (most recent call last):
      ...
      KeyError: 'stephan'

    Make sure it is really gone after committing:

      >>> transaction.commit()
      >>> dm.root['c'].keys()
      []
    """


def doctest_MongoContainer_basic():
    """MongoContainer: basic

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Let's add a container to the root:

      >>> transaction.commit()
      >>> dm.root['c'] = container.MongoContainer('person')

      >>> db = dm._conn[DBNAME]
      >>> pprint(list(db['mongopersist.zope.container.MongoContainer'].find()))
      [{u'_id': ObjectId('4e7ddf12e138237403000000'),
        u'_m_collection': u'person'}]

    It is unfortunate that the '_m_collection' attribute is set. This is
    avoidable using a sub-class.

      >>> dm.root['c'][u'stephan'] = Person(u'Stephan')
      ContainerModifiedEvent: <...MongoContainer ...>
      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c'][u'stephan']
      <Person Stephan>

      >>> dm.root['c']['stephan'].__parent__
      <mongopersist.zope.container.MongoContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      u'stephan'

    It is a feature of the container that the item is immediately available
    after assignment, but before the data is stored in the database. Let's
    commit and access the data again:

      >>> transaction.commit()

      >>> pprint(list(db['person'].find()))
      [{u'_id': ObjectId('4e7e9d3ae138232d7b000003'),
        u'key': u'stephan',
        u'name': u'Stephan',
        u'parent': DBRef(u'mongopersist.zope.container.MongoContainer',
                         ObjectId('4e7e9d3ae138232d7b000000'),
                         u'mongopersist_container_test')}]

      >>> dm.root['c'].keys()
      [u'stephan']
      >>> dm.root['c']['stephan'].__parent__
      <mongopersist.zope.container.MongoContainer object at 0x7fec50f86500>
      >>> dm.root['c']['stephan'].__name__
      'stephan'

    We get a usual key error, if an object does not exist:

      >>> dm.root['c']['roy']
      Traceback (most recent call last):
      ...
      KeyError: 'roy'

    Now remove the item:

      >>> del dm.root['c']['stephan']
      ContainerModifiedEvent: <...MongoContainer ...>

    The changes are immediately visible.

      >>> dm.root['c'].keys()
      []
      >>> dm.root['c']['stephan']
      Traceback (most recent call last):
      ...
      KeyError: 'stephan'

    Make sure it is really gone after committing:

      >>> transaction.commit()
      >>> dm.root['c'].keys()
      []
    """

def doctest_MongoContainer_constructor():
    """MongoContainer: constructor

    The constructor of the MongoContainer class has several advanced arguments
    that allow customizing the storage options.

      >>> transaction.commit()
      >>> c = container.MongoContainer(
      ...     'person',
      ...     database = 'testdb',
      ...     mapping_key = 'name',
      ...     parent_key = 'site')

    The database allows you to specify a custom database in which the items
    are located. Otherwise the datamanager's default database is used.

      >>> c._m_database
      'testdb'

    The mapping key is the key/attribute of the contained items in which their
    name/key within the mapping is stored.

      >>> c._m_mapping_key
      'name'

    The parent key is the key/attribute in which the parent reference is
    stored. This is used to suport multiple containers per Mongo collection.

      >>> c._m_parent_key
      'site'
    """
def doctest_MongoContainer_m_parent_key_value():
    r"""MongoContainer: _m_parent_key_value()

    This method is used to extract the parent refernce for the item.

      >>> c = container.MongoContainer('person')

    The default implementation requires the container to be in some sort of
    persistent store, though it does not care whether this store is Mongo or a
    classic ZODB. This feature allows one to mix and match ZODB and Mongo
    storage.

      >>> c._m_get_parent_key_value()
      Traceback (most recent call last):
      ...
      ValueError: _p_jar not found.

    Now the ZODB case:

      >>> c._p_jar = object()
      >>> c._p_oid = '\x00\x00\x00\x00\x00\x00\x00\x01'
      >>> c._m_get_parent_key_value()
      'zodb-0000000000000001'

    And finally the Mongo case:

      >>> c._p_jar = c._p_oid = None
      >>> dm.root['people'] = c
      >>> c._m_get_parent_key_value()
      <mongopersist.zope.container.MongoContainer object at 0x32deed8>

    In that final case, the container itself is returned, because upon
    serialization, we simply look up the dbref.
    """

def doctest_MongoContainer_many_items():
    """MongoContainer: many items

    Let's create an interesting set of data:

      >>> transaction.commit()
      >>> dm.root['people'] = container.MongoContainer('person')
      >>> dm.root['people'][u'stephan'] = Person(u'Stephan')
      >>> dm.root['people'][u'roy'] = Person(u'Roy')
      >>> dm.root['people'][u'roger'] = Person(u'Roger')
      >>> dm.root['people'][u'adam'] = Person(u'Adam')
      >>> dm.root['people'][u'albertas'] = Person(u'Albertas')
      >>> dm.root['people'][u'russ'] = Person(u'Russ')

    In order for find to work, the data has to be committed:

      >>> transaction.commit()

    Let's now search and receive documents as result:

      >>> sorted(dm.root['people'].keys())
      [u'adam', u'albertas', u'roger', u'roy', u'russ', u'stephan']
      >>> dm.root['people'][u'stephan']
      <Person Stephan>
      >>> dm.root['people'][u'adam']
      <Person Adam>
"""

def doctest_MongoContainer_find():
    """MongoContainer: find

    The Mongo Container supports direct Mongo queries. It does, however,
    insert the additional container filter arguments and can optionally
    convert the documents to objects.

    Let's create an interesting set of data:

      >>> transaction.commit()
      >>> dm.root['people'] = container.MongoContainer('person')
      >>> dm.root['people'][u'stephan'] = Person(u'Stephan')
      >>> dm.root['people'][u'roy'] = Person(u'Roy')
      >>> dm.root['people'][u'roger'] = Person(u'Roger')
      >>> dm.root['people'][u'adam'] = Person(u'Adam')
      >>> dm.root['people'][u'albertas'] = Person(u'Albertas')
      >>> dm.root['people'][u'russ'] = Person(u'Russ')

    In order for find to work, the data has to be committed:

      >>> transaction.commit()

    Let's now search and receive documents as result:

      >>> res = dm.root['people'].raw_find({'name': {'$regex': '^Ro.*'}})
      >>> pprint(list(res))
      [{u'_id': ObjectId('4e7eb152e138234158000004'),
        u'key': u'roy',
        u'name': u'Roy',
        u'parent': DBRef(u'mongopersist.zope.container.MongoContainer',
                         ObjectId('4e7eb152e138234158000000'),
                         u'mongopersist_container_test')},
       {u'_id': ObjectId('4e7eb152e138234158000005'),
        u'key': u'roger',
        u'name': u'Roger',
        u'parent': DBRef(u'mongopersist.zope.container.MongoContainer',
                         ObjectId('4e7eb152e138234158000000'),
                         u'mongopersist_container_test')}]

    And now the same query, but this time with object results:

      >>> res = dm.root['people'].find({'name': {'$regex': '^Ro.*'}})
      >>> pprint(list(res))
      [<Person Roy>, <Person Roger>]

    When no spec is specified, all items are returned:

      >>> res = dm.root['people'].find()
      >>> pprint(list(res))
      [<Person Stephan>, <Person Roy>, <Person Roger>, <Person Adam>,
       <Person Albertas>, <Person Russ>]

    You can also search for a single result:

      >>> res = dm.root['people'].raw_find_one({'name': {'$regex': '^St.*'}})
      >>> pprint(res)
      {u'_id': ObjectId('4e7eb259e138234289000003'),
       u'key': u'stephan',
       u'name': u'Stephan',
       u'parent': DBRef(u'mongopersist.zope.container.MongoContainer',
                        ObjectId('4e7eb259e138234289000000'),
                        u'mongopersist_container_test')}

      >>> stephan = dm.root['people'].find_one({'name': {'$regex': '^St.*'}})
      >>> pprint(stephan)
      <Person Stephan>

    If no result is found, ``None`` is returned:

      >>> dm.root['people'].find_one({'name': {'$regex': '^XXX.*'}})

    If there is no spec, then simply the first item is returned:

      >>> dm.root['people'].find_one()
      <Person Stephan>

    On the other hand, if the spec is an id, we look for it instead:

      >>> dm.root['people'].find_one(stephan._p_oid.id)
      <Person Stephan>
    """

def doctest_AllItemsMongoContainer_basic():
    """AllItemsMongoContainer: basic

    This type of container returns all items of the collection without regard
    of a parenting hierarchy.

    Let's start by creating two person containers that service different
    purposes:

      >>> transaction.commit()

      >>> dm.root['friends'] = container.MongoContainer('person')
      >>> dm.root['friends'][u'roy'] = Person(u'Roy')
      >>> dm.root['friends'][u'roger'] = Person(u'Roger')

      >>> dm.root['family'] = container.MongoContainer('person')
      >>> dm.root['family'][u'anton'] = Person(u'Anton')
      >>> dm.root['family'][u'konrad'] = Person(u'Konrad')

      >>> transaction.commit()
      >>> sorted(dm.root['friends'].keys())
      [u'roger', u'roy']
      >>> sorted(dm.root['family'].keys())
      [u'anton', u'konrad']

    Now we can create an all-items-container that allows us to view all
    people.

      >>> dm.root['all-people'] = container.AllItemsMongoContainer('person')
      >>> sorted(dm.root['all-people'].keys())
      [u'anton', u'konrad', u'roger', u'roy']
    """

def doctest_SubDocumentMongoContainer_basic():
    r"""SubDocumentMongoContainer: basic

    Let's make sure events are fired correctly:

      >>> zope.component.provideHandler(handleObjectModifiedEvent)

    Sub_document Mongo containers are useful, since they avoid the creation of
    a commonly trivial collections holding meta-data for the collection
    object. But they require a root document:

      >>> dm.reset()
      >>> dm.root['app_root'] = ApplicationRoot()

    Let's add a container to the app root:

      >>> dm.root['app_root']['people'] = \
      ...     container.SubDocumentMongoContainer('person')
      ContainerModifiedEvent: <ApplicationRoot>

      >>> transaction.commit()
      >>> db = dm._conn[DBNAME]
      >>> pprint(list(db['root'].find()))
      [{u'_id': ObjectId('4e7ea67be138233711000001'),
        u'data':
         {u'people':
          {u'_m_collection': u'person',
           u'_py_persistent_type':
               u'mongopersist.zope.container.SubDocumentMongoContainer'}}}]

    It is unfortunate that the '_m_collection' attribute is set. This is
    avoidable using a sub-class. Let's make sure the container can be loaded
    correctly:

      >>> dm.root['app_root']['people']
      <mongopersist.zope.container.SubDocumentMongoContainer ...>
      >>> dm.root['app_root']['people'].__parent__
      <ApplicationRoot>
      >>> dm.root['app_root']['people'].__name__
      'people'

    Let's add an item to the container:

      >>> dm.root['app_root']['people'][u'stephan'] = Person(u'Stephan')
      ContainerModifiedEvent: <...SubDocumentMongoContainer ...>
      >>> dm.root['app_root']['people'].keys()
      [u'stephan']
      >>> dm.root['app_root']['people'][u'stephan']
      <Person Stephan>

      >>> transaction.commit()
      >>> dm.root['app_root']['people'].keys()
      [u'stephan']
    """

def doctest_MongoContainer_with_ZODB():
    r"""MongoContainer: with ZODB

    This test demonstrates how a Mongo Container lives inside a ZODB tree:

      >>> zodb = ZODB.DB(ZODB.DemoStorage.DemoStorage())
      >>> root = zodb.open().root()
      >>> root['app'] = btree.BTreeContainer()
      >>> root['app']['people'] = container.MongoContainer('person')

    Let's now commit the transaction and make sure everything is cool.

      >>> transaction.commit()
      >>> root = zodb.open().root()
      >>> root['app']
      <zope.container.btree.BTreeContainer object at 0x7fbb5842f578>
      >>> root['app']['people']
      <mongopersist.zope.container.MongoContainer object at 0x7fd6e23555f0>

    Trying accessing people fails:

      >>> root['app']['people'].keys()
      Traceback (most recent call last):
      ...
      ComponentLookupError:
       (<InterfaceClass mongopersist.interfaces.IMongoDataManagerProvider>, '')

    This is because we have not told the system how to get a datamanager:

      >>> class Provider(object):
      ...     zope.interface.implements(interfaces.IMongoDataManagerProvider)
      ...     def get(self):
      ...         return dm
      >>> zope.component.provideUtility(Provider())

    So let's try again:

      >>> root['app']['people'].keys()
      []

    Next we create a person object and make sure it gets properly persisted.

      >>> root['app']['people']['stephan'] = Person(u'Stephan')
      >>> transaction.commit()
      >>> root = zodb.open().root()
      >>> root['app']['people'].keys()
      [u'stephan']

      >>> stephan = root['app']['people']['stephan']
      >>> stephan.__name__
      'stephan'
      >>> stephan.__parent__
      <mongopersist.zope.container.MongoContainer object at 0x7f6b6273b7d0>

      >>> pprint(list(dm._conn[DBNAME]['person'].find()))
      [{u'_id': ObjectId('4e7ed795e1382366a0000001'),
        u'key': u'stephan',
        u'name': u'Stephan',
        u'parent': u'zodb-1058e89d27d8afd9'}]

    Note that we produced a nice hex-presentation of the ZODB's OID.
    """

checker = renormalizing.RENormalizing([
    (re.compile(r'datetime.datetime(.*)'),
     'datetime.datetime(2011, 10, 1, 9, 45)'),
    (re.compile(r"ObjectId\('[0-9a-f]*'\)"),
     "ObjectId('4e7ddf12e138237403000000')"),
    (re.compile(r"object at 0x[0-9a-f]*>"),
     "object at 0x001122>"),
    (re.compile(r"zodb-[0-9a-f].*"),
     "zodb-01af3b00c5"),
    ])

@zope.component.adapter(
    zope.interface.Interface,
    zope.lifecycleevent.interfaces.IObjectModifiedEvent
    )
def handleObjectModifiedEvent(object, event):
    print event.__class__.__name__+':', repr(object)


def setUp(test):
    placelesssetup.setUp(test)
    module.setUp(test)
    test.globs['conn'] = pymongo.Connection('localhost', 27017, tz_aware=False)
    test.globs['DBNAME'] = 'mongopersist_container_test'
    test.globs['conn'].drop_database(test.globs['DBNAME'])
    test.globs['dm'] = datamanager.MongoDataManager(
        test.globs['conn'],
        default_database=test.globs['DBNAME'],
        root_database=test.globs['DBNAME'])

def tearDown(test):
    placelesssetup.tearDown(test)
    module.tearDown(test)
    test.globs['conn'].disconnect()
    serialize.SERIALIZERS.__init__()

def test_suite():
    return doctest.DocTestSuite(
        setUp=setUp, tearDown=tearDown, checker=checker,
        optionflags=(doctest.NORMALIZE_WHITESPACE|
                     doctest.ELLIPSIS|
                     doctest.REPORT_ONLY_FIRST_FAILURE
                     #|doctest.REPORT_NDIFF
                     )
        )
