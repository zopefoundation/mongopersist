======================
Mongo Data Persistence
======================

This document outlines the general capabilities of the ``mongopersist``
package. ``mongopersist`` is a Mongo storage implementation for persistent
Python objects. It is *not* a storage for the ZODB.

The goal of ``mongopersist`` is to provide a data manager that serializes
objects to Mongo at transaction boundaries. The mongo data manager is a
persistent data manager, which handles events at transaction boundaries (see
``transaction.interfaces.IDataManager``) as well as events from the
persistency framework (see ``persistent.interfaces.IPersistentDataManager``).

An instance of a data manager is supposed to have the same life time as the
transaction, meaning that it is assumed that you create a new data manager
when creating a new transaction:

  >>> import transaction

Note: The ``conn`` object is a ``pymongo.connection.Connection`` instance. In
this case our tests use the ``mongopersist_test`` database.

Let's now define a simple persistent object:

  >>> import datetime
  >>> import persistent

  >>> class Person(persistent.Persistent):
  ...
  ...     def __init__(self, name, phone=None, address=None, friends=None,
  ...                  visited=(), birthday=None):
  ...         self.name = name
  ...         self.address = address
  ...         self.friends = friends or {}
  ...         self.visited = visited
  ...         self.phone = phone
  ...         self.birthday = birthday
  ...         self.today = datetime.datetime.now()
  ...
  ...     def __str__(self):
  ...         return self.name
  ...
  ...     def __repr__(self):
  ...         return '<%s %s>' %(self.__class__.__name__, self)

We will fill out the other objects later. But for now, let's create a new
person and store it in Mongo:

  >>> stephan = Person(u'Stephan')
  >>> stephan
  <Person Stephan>

The datamanager provides a ``root`` attribute in which the object tree roots
can be stored. It is special in the sense that it immediately writes the data
to the DB:

  >>> dm.root['stephan'] = stephan
  >>> dm.root['stephan']
  <Person Stephan>

Custom Persistence Collections
------------------------------

By default, persistent objects are stored in a collection having the Python
path of the class:

  >>> from mongopersist import serialize
  >>> person_cn = serialize.get_dotted_name(Person)
  >>> person_cn
  '__main__.Person'

  >>> import pprint
  >>> pprint.pprint(list(conn[DBNAME][person_cn].find()))
  [{u'_id': ObjectId('4e7ddf12e138237403000000'),
    u'address': None,
    u'birthday': None,
    u'friends': {},
    u'name': u'Stephan',
    u'phone': None,
    u'today': datetime.datetime(2011, 10, 1, 9, 45),
    u'visited': []}]

As you can see, the stored document for the person looks very Mongo. But oh
no, I forgot to specify the full name for Stephan. Let's do that:

  >>> dm.root['stephan'].name = u'Stephan Richter'

This time, the data is not automatically saved:

  >>> conn[DBNAME][person_cn].find_one()['name']
  u'Stephan'

So we have to commit the transaction first:

  >>> transaction.commit()
  >>> conn[DBNAME][person_cn].find_one()['name']
  u'Stephan Richter'

Let's now add an address for Stephan. Addresses are also persistent objects:

  >>> class Address(persistent.Persistent):
  ...     _p_mongo_collection = 'address'
  ...
  ...     def __init__(self, city, zip):
  ...         self.city = city
  ...         self.zip = zip
  ...
  ...     def __str__(self):
  ...         return '%s (%s)' %(self.city, self.zip)
  ...
  ...     def __repr__(self):
  ...         return '<%s %s>' %(self.__class__.__name__, self)

MongoPersist supports a special attribute called ``_p_mongo_collection``,
which allows you to specify a custom collection to use.

  >>> dm.root['stephan'].address = Address('Maynard', '01754')
  >>> dm.root['stephan'].address
  <Address Maynard (01754)>

Note that the address is not immediately saved in the database:

  >>> list(conn[DBNAME]['address'].find())
  []

But once we commit the transaction, everything is available:

  >>> transaction.commit()
  >>> pprint.pprint(list(conn[DBNAME]['address'].find()))
  [{u'_id': ObjectId('4e7de388e1382377f4000003'),
    u'city': u'Maynard',
    u'zip': u'01754'}]

  >>> pprint.pprint(list(conn[DBNAME][person_cn].find()))
  [{u'_id': ObjectId('4e7ddf12e138237403000000'),
    u'address': DBRef(u'address',
                      ObjectId('4e7ddf12e138237403000000'),
                      u'mongopersist_test'),
    u'birthday': None,
    u'friends': {},
    u'name': u'Stephan Richter',
    u'phone': None,
    u'today': datetime.datetime(2011, 10, 1, 9, 45)
    u'visited': []}]

  >>> dm.root['stephan'].address
  <Address Maynard (01754)>


Non-Persistent Objects
----------------------

As you can see, even the reference looks nice and uses the standard Mongo DB
reference construct. But what about arbitrary non-persistent, but pickable,
objects? Well, let's create a phone number object for that:

  >>> class Phone(object):
  ...
  ...     def __init__(self, country, area, number):
  ...         self.country = country
  ...         self.area = area
  ...         self.number = number
  ...
  ...     def __str__(self):
  ...         return '%s-%s-%s' %(self.country, self.area, self.number)
  ...
  ...     def __repr__(self):
  ...         return '<%s %s>' %(self.__class__.__name__, self)

  >>> dm.root['stephan'].phone = Phone('+1', '978', '394-5124')
  >>> dm.root['stephan'].phone
  <Phone +1-978-394-5124>

Let's now commit the transaction and look at the Mongo document again:

  >>> transaction.commit()
  >>> dm.root['stephan'].phone
  <Phone +1-978-394-5124>

  >>> pprint.pprint(list(conn[DBNAME][person_cn].find()))
  [{u'_id': ObjectId('4e7ddf12e138237403000000'),
    u'address': DBRef(u'address',
                      ObjectId('4e7ddf12e138237403000000'),
                      u'mongopersist_test'),
    u'birthday': None,
    u'friends': {},
    u'name': u'Stephan Richter',
    u'phone': {u'_py_type': u'__main__.Phone',
               u'area': u'978',
               u'country': u'+1',
               u'number': u'394-5124'},
    u'today': datetime.datetime(2011, 10, 1, 9, 45)
    u'visited': []}]

As you can see, for arbitrary non-persistent objects we need a small hint in
the sub-document, but it is very minimal. If the ``__reduce__`` method returns
a more complex construct, more meta-data is written. We will see that next
when storing a date and other arbitrary data:

  >>> dm.root['stephan'].friends = {'roy': Person(u'Roy Mathew')}
  >>> dm.root['stephan'].visited = (u'Germany', u'USA')
  >>> dm.root['stephan'].birthday = datetime.date(1980, 1, 25)

  >>> transaction.commit()
  >>> dm.root['stephan'].friends
  {u'roy': <Person Roy Mathew>}
  >>> dm.root['stephan'].visited
  [u'Germany', u'USA']
  >>> dm.root['stephan'].birthday
  datetime.date(1980, 1, 25)

As you can see, a dictionary key is always converted to unicode and tuples are
always maintained as lists, since BSON does not have two sequence types.

  >>> pprint.pprint(conn[DBNAME][person_cn].find_one(
  ...     {'name': 'Stephan Richter'}))
  {u'_id': ObjectId('4e7df744e138230a3e000000'),
   u'address': DBRef(u'address',
                     ObjectId('4e7df744e138230a3e000003'),
                     u'mongopersist_test'),
   u'birthday': {u'_py_factory': u'datetime.date',
                 u'_py_factory_args': [Binary('\x07\xbc\x01\x19', 0)]},
   u'friends': {u'roy': DBRef(u'__main__.Person',
                              ObjectId('4e7df745e138230a3e000004'),
                              u'mongopersist_test')},
   u'name': u'Stephan Richter',
   u'phone': {u'_py_type': u'__main__.Phone',
              u'area': u'978',
              u'country': u'+1',
              u'number': u'394-5124'},
   u'today': datetime.datetime(2011, 9, 24, 11, 29, 8, 930000),
   u'visited': [u'Germany', u'USA']}


Custom Serializers
------------------

As you can see, the serialization of the birthay is all but ideal. We can,
however, provide a custom serializer that uses the ordinal to store the data.

  >>> class DateSerializer(serialize.ObjectSerializer):
  ...
  ...     def can_read(self, state):
  ...         return isinstance(state, dict) and \
  ...                state.get('_py_type') == 'datetime.date'
  ...
  ...     def read(self, state):
  ...         return datetime.date.fromordinal(state['ordinal'])
  ...
  ...     def can_write(self, obj):
  ...         return isinstance(obj, datetime.date)
  ...
  ...     def write(self, obj):
  ...         return {'_py_type': 'datetime.date',
  ...                 'ordinal': obj.toordinal()}

  >>> serialize.SERIALIZERS.append(DateSerializer())
  >>> dm.root['stephan']._p_changed = True
  >>> transaction.commit()

Let's have a look again:

  >>> dm.root['stephan'].birthday
  datetime.date(1980, 1, 25)

  >>> pprint.pprint(conn[DBNAME][person_cn].find_one(
  ...     {'name': 'Stephan Richter'}))
  {u'_id': ObjectId('4e7df803e138230aeb000000'),
   u'address': DBRef(u'address',
                     ObjectId('4e7df803e138230aeb000003'),
                     u'mongopersist_test'),
   u'birthday': {u'_py_type': u'datetime.date', u'ordinal': 722839},
   u'friends': {u'roy': DBRef(u'__main__.Person',
                              ObjectId('4e7df803e138230aeb000004'),
                              u'mongopersist_test')},
   u'name': u'Stephan Richter',
   u'phone': {u'_py_type': u'__main__.Phone',
              u'area': u'978',
              u'country': u'+1',
              u'number': u'394-5124'},
   u'today': datetime.datetime(2011, 9, 24, 11, 32, 19, 640000),
   u'visited': [u'Germany', u'USA']}

Much better!


Persistent Objects as Sub-Documents
-----------------------------------

In order to give more control over which objects receive their own collections
and which do not, the developer can provide a special flag marking a
persistent class so that it becomes part of its parent object's document:

  >>> class Car(persistent.Persistent):
  ...     _p_mongo_sub_object = True
  ...
  ...     def __init__(self, year, make, model):
  ...         self.year = year
  ...         self.make = make
  ...         self.model = model
  ...
  ...     def __str__(self):
  ...         return '%s %s %s' %(self.year, self.make, self.model)
  ...
  ...     def __repr__(self):
  ...         return '<%s %s>' %(self.__class__.__name__, self)

The ``_p_mongo_sub_object`` is used to mark a type of object to be just part
of another document:

  >>> dm.root['stephan'].car = car = Car('2005', 'Ford', 'Explorer')
  >>> transaction.commit()

  >>> dm.root['stephan'].car
  <Car 2005 Ford Explorer>

  >>> pprint.pprint(conn[DBNAME][person_cn].find_one(
  ...     {'name': 'Stephan Richter'}))
  {u'_id': ObjectId('4e7dfac7e138230d3d000000'),
   u'address': DBRef(u'address',
                     ObjectId('4e7dfac7e138230d3d000003'),
                     u'mongopersist_test'),
   u'birthday': {u'_py_type': u'datetime.date', u'ordinal': 722839},
   u'car': {u'_py_persistent_type': u'__main__.Car',
            u'make': u'Ford',
            u'model': u'Explorer',
            u'year': u'2005'},
   u'friends': {u'roy': DBRef(u'__main__.Person',
                              ObjectId('4e7dfac7e138230d3d000004'),
                              u'mongopersist_test')},
   u'name': u'Stephan Richter',
   u'phone': {u'_py_type': u'__main__.Phone',
              u'area': u'978',
              u'country': u'+1',
              u'number': u'394-5124'},
   u'today': datetime.datetime(2011, 9, 24, 11, 44, 7, 662000),
   u'visited': [u'Germany', u'USA']}

The reason we want objects to be persistent is so that they pick up changes
automatically:

  >>> dm.root['stephan'].car.year = '2004'
  >>> transaction.commit()
  >>> dm.root['stephan'].car
  <Car 2004 Ford Explorer>


Collection Sharing
------------------

Since Mongo is so flexible, it sometimes makes sense to store multiple types
of (similar) objects in the same collection. In those cases you instruct the
object type to store its Python path as part of the document.

Warning: Please note though that this method is less efficient, since the
document must be loaded in order to create a ghost causing more database
access.

  >>> class ExtendedAddress(Address):
  ...
  ...     def __init__(self, city, zip, country):
  ...         super(ExtendedAddress, self).__init__(city, zip)
  ...         self.country = country
  ...
  ...     def __str__(self):
  ...         return '%s (%s) in %s' %(self.city, self.zip, self.country)

In order to accomplish collection sharing, you simply create another class
that has the same ``_p_mongo_collection`` string as another (sub-classing will
ensure that).

So let's give Stephan an extended address now.

  >>> dm.root['stephan'].address2 = ExtendedAddress(
  ...     'Tettau', '01945', 'Germany')
  >>> dm.root['stephan'].address2
  <ExtendedAddress Tettau (01945) in Germany>
  >>> transaction.commit()

When loading the addresses, they should be of the right type:

  >>> dm.root['stephan'].address
  <Address Maynard (01754)>
  >>> dm.root['stephan'].address2
  <ExtendedAddress Tettau (01945) in Germany>


Tricky Cases
------------

Changes in Basic Mutable Type
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Tricky, tricky. How do we make the framework detect changes in mutable
objects, such as lists and dictionaries? Answer: We keep track of which
persistent object they belong to and provide persistent implementations.

  >>> type(dm.root['stephan'].friends)
   <class 'mongopersist.serialize.PersistentDict'>

  >>> dm.root['stephan'].friends[u'roger'] = Person(u'Roger')
  >>> transaction.commit()
  >>> dm.root['stephan'].friends.keys()
  [u'roy', u'roger']

The same is true for lists:

  >>> type(dm.root['stephan'].visited)
   <class 'mongopersist.serialize.PersistentList'>

  >>> dm.root['stephan'].visited.append('France')
  >>> transaction.commit()
  >>> dm.root['stephan'].visited
  [u'Germany', u'USA', u'France']


Circular Non-Persistent References
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Any mutable object that is stored in a sub-document, cannot have multiple
references in the object tree, since there is no global referencing. These
circular references are detected and reported:

  >>> class Top(persistent.Persistent):
  ...     foo = None

  >>> class Foo(object):
  ...     bar = None

  >>> class Bar(object):
  ...     foo = None

  >>> top = Top()
  >>> foo = Foo()
  >>> bar = Bar()
  >>> top.foo = foo
  >>> foo.bar = bar
  >>> bar.foo = foo

  >>> dm.root['top'] = top
  Traceback (most recent call last):
  ...
  CircularReferenceError: <__main__.Foo object at 0x7fec75731890>


Circular Persistent References
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In general, circular references among persistent objects are not a problem,
since we always only store a link to the object. However, there is a case when
the circular dependencies become a problem.

If you set up an object tree with circular references and then add the tree to
the storage at once, it must insert objects during serialization, so that
references can be created. However, care needs to be taken to only create a
minimal reference object, so that the system does not try to recursively
reduce the state.

  >>> class PFoo(persistent.Persistent):
  ...     bar = None

  >>> class PBar(persistent.Persistent):
  ...     foo = None

  >>> top = Top()
  >>> foo = PFoo()
  >>> bar = PBar()
  >>> top.foo = foo
  >>> foo.bar = bar
  >>> bar.foo = foo

  >>> dm.root['ptop'] = top


Containers and Collections
--------------------------

Now that we have talked so much about the gory details on storing one object,
what about mappings that reflect an entire collection, for example a
collection of people.

There are many approaches that can be taken. The folowing implementation
defines an attribute in the document as the mapping key and names a
collection:

  >>> from mongopersist import mapping
  >>> class People(mapping.MongoCollectionMapping):
  ...     __mongo_collection__ = person_cn
  ...     __mongo_mapping_key__ = 'short_name'

The mapping takes the data manager as an argument. One can easily create a
sub-class that assigns the data manager automatically. Let's have a look:

  >>> People(dm).keys()
  []

The reason no person is in the list yet, is because no document has the key
yet or the key is null. Let's change that:

  >>> People(dm)['stephan'] = dm.root['stephan']
  >>> transaction.commit()

  >>> People(dm).keys()
  [u'stephan']
  >>> People(dm)['stephan']
  <Person Stephan Richter>

Also note that setting the "short-name" attribute on any other person will add
it to the mapping:

  >>> dm.root['stephan'].friends['roy'].short_name = 'roy'
  >>> transaction.commit()
  >>> People(dm).keys()
  [u'roy', u'stephan']


Write-Conflict Detection
------------------------

Since Mongo has no support for MVCC, it does not provide a concept of write
conflict detection. However, a simple write-conflict detection can be easily
implemented using a serial number on the document.

Let's reset the database and create a data manager with enabled conflict
detection:

  >>> from mongopersist import datamanager
  >>> conn.drop_database(DBNAME)
  >>> dm2 = datamanager.MongoDataManager(
  ...     conn,
  ...     default_database=DBNAME,
  ...     root_database=DBNAME,
  ...     detect_conflicts=True)

Now we add a person and see that the serial got stored.

  >>> dm2.root['stephan'] = Person(u'Stephan')
  >>> dm2.root['stephan']._p_serial
  '\x00\x00\x00\x00\x00\x00\x00\x01'
  >>> pprint.pprint(dm2._conn[DBNAME][person_cn].find_one())
  {u'_id': ObjectId('4e7fe18de138233a5b000009'),
   u'_py_serial': 1,
   u'address': None,
   u'birthday': None,
   u'friends': {},
   u'name': u'Stephan',
   u'phone': None,
   u'today': datetime.datetime(2011, 9, 25, 22, 21, 1, 656000),
   u'visited': []}

Next we change the person and commit it again:

  >>> dm2.root['stephan'].name = u'Stephan <Unknown>'
  >>> transaction.commit()
  >>> pprint.pprint(dm2._conn[DBNAME][person_cn].find_one())
  {u'_id': ObjectId('4e7fe18de138233a5b000009'),
   u'_py_serial': 2,
   u'address': None,
   u'birthday': None,
   u'friends': {},
   u'name': u'Stephan <Unknown>',
   u'phone': None,
   u'today': datetime.datetime(2011, 9, 25, 22, 21, 1, 656000),
   u'visited': []}

Let's now start a new transaction with some modifications:

  >>> dm2.root['stephan'].name = u'Stephan Richter'

However, in the mean time another transaction modifies the object. (We will do
this here directly via Mongo for simplicity.)

  >>> dm2._conn[DBNAME][person_cn].update(
  ...     {'name': u'Stephan <Unknown>'},
  ...     {'$set': {'name': u'Stephan R.', '_py_serial': 3}})
  >>> pprint.pprint(dm2._conn[DBNAME][person_cn].find_one())
  {u'_id': ObjectId('4e7fe1f4e138233ac4000009'),
   u'_py_serial': 3,
   u'address': None,
   u'birthday': None,
   u'friends': {},
   u'name': u'Stephan R.',
   u'phone': None,
   u'today': datetime.datetime(2011, 9, 25, 22, 22, 44, 343000),
   u'visited': []}

Now our changing transaction tries to commit:

  >>> transaction.commit()
  Traceback (most recent call last):
  ...
  ConflictError: database conflict error
      (oid ..., class Person, start serial 2, current serial 3)

  >>> transaction.abort()
