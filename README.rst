mongopersist
============

A python mongoDB Persistence Backend.

Providing transparent persistence of python objects.

This document outlines the general capabilities of the ``mongopersist``
package. ``mongopersist`` is a mongoDB storage implementation for persistent
Python objects. It is *NOT* a storage for the ZODB.

The goal of ``mongopersist`` is to provide a data manager that serializes
objects to mongoDB at transaction boundaries. The mongo data manager is a
persistent data manager, which handles events at transaction boundaries (see
``transaction.interfaces.IDataManager``) as well as events from the
persistency framework (see ``persistent.interfaces.IPersistentDataManager``).

An instance of a data manager is supposed to have the same life time as the
transaction, meaning that it is assumed that you create a new data manager
when creating a new transaction:

  >>> import transaction

Let's now define a simple persistent object:

  >>> import persistent

  >>> class Person(persistent.Persistent, ReprMixin):
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

Let's create a new person and store it in mongoDB:

  >>> stephan = Person(u'Stephan')
  >>> dm.root['stephan'] = stephan

By default, persistent objects are stored in a collection having the Python
path of the class.
Let's see what got stored in mongoDB:

  >>> dumpCollection('__main__.Person')
  [{u'_id': ObjectId('51c0571eb25d2b2de8325726'),
    u'address': None,
    u'birthday': None,
    u'friends': {},
    u'name': u'Stephan',
    u'phone': None,
    u'today': datetime.datetime(2013, 6, 18, 14, 48, 30, 970000),
    u'visited': []}]

Let's now add an address for Stephan. Addresses are also persistent objects:

  >>> class Address(persistent.Persistent, ReprMixin):
  ...     _p_mongo_collection = 'address'
  ...
  ...     def __init__(self, city, zip):
  ...         self.city = city
  ...         self.zip = zip
  ...
  ...     def __str__(self):
  ...         return '%s (%s)' %(self.city, self.zip)

  >>> stephan.address = Address('Maynard', '01754')

We need to commit the transaction, to push the data to mongoDB:

  >>> transaction.commit()

  >>> dumpCollection('address')
  [{u'_id': ObjectId('51c05809b25d2b2e4f90cbdd'),
    u'city': u'Maynard',
    u'zip': u'01754'}]

As you can see, even the reference to the Address object looks nice and uses
the standard mongoDB reference construct.

  >>> dumpCollection('__main__.Person')
  [{u'_id': ObjectId('51c05819b25d2b2ea58a4e55'),
    u'address': DBRef(u'address', ObjectId('51c05819b25d2b2ea58a4e58'), u'mongopersist_test'),
    u'birthday': None,
    u'friends': {},
    u'name': u'Stephan',
    u'phone': None,
    u'today': datetime.datetime(2013, 6, 18, 14, 52, 41, 133000),
    u'visited': []}]

But what about arbitrary non-persistent, but picklable, objects?
Well, let's create a phone number object for that:

  >>> class Phone(ReprMixin):
  ...
  ...     def __init__(self, country, area, number):
  ...         self.country = country
  ...         self.area = area
  ...         self.number = number
  ...
  ...     def __str__(self):
  ...         return '%s-%s-%s' %(self.country, self.area, self.number)

  >>> stephan = dm.root['stephan']
  >>> stephan.phone = Phone('+1', '978', '394-5124')
  >>> transaction.commit()

  >>> dumpCollection('__main__.Person')
  [{u'_id': ObjectId('51c059beb25d2b3157bf5adf'),
    u'address': DBRef(u'address', ObjectId('51c059beb25d2b3157bf5ae2'), u'mongopersist_test'),
    u'birthday': None,
    u'friends': {},
    u'name': u'Stephan',
    u'phone': {u'_py_type': u'__main__.Phone',
               u'area': u'978',
               u'country': u'+1',
               u'number': u'394-5124'},
    u'today': datetime.datetime(2013, 6, 18, 14, 59, 42, 554000),
    u'visited': []}]

Let's now set various attributes:

  >>> stephan = dm.root['stephan']
  >>> stephan.friends = {'roy': Person(u'Roy Mathew')}
  >>> stephan.visited = (u'Germany', u'USA')
  >>> stephan.birthday = datetime.date(1980, 1, 25)

Push the data to mongoDB, and dump the results:

  >>> transaction.commit()
  >>> dumpCollection('__main__.Person')
  [{u'_id': ObjectId('4e7ddf12e138237403000000'),
    u'address': DBRef(u'address', ObjectId('4e7ddf12e138237403000000'), u'mongopersist_test'),
    u'birthday': {u'_py_factory': u'datetime.date',
                  u'_py_factory_args': [Binary('\x07\xbc\x01\x19', 0)]},
    u'friends': {u'roy': DBRef(u'__main__.Person', ObjectId('4e7ddf12e138237403000000'), u'mongopersist_test')},
    u'name': u'Stephan',
    u'phone': {u'_py_type': u'__main__.Phone',
               u'area': u'978',
               u'country': u'+1',
               u'number': u'394-5124'},
    u'today': datetime.datetime(2011, 10, 1, 9, 45)
    u'visited': [u'Germany', u'USA']},
   {u'_id': ObjectId('4e7ddf12e138237403000000'),
    u'address': None,
    u'birthday': None,
    u'friends': {},
    u'name': u'Roy Mathew',
    u'phone': None,
    u'today': datetime.datetime(2011, 10, 1, 9, 45)
    u'visited': []}]

Of course all properties can be retrieved as python objects:

  >>> stephan = dm.root['stephan']
  >>> stephan.address
  <Address Maynard (01754)>

  >>> stephan.address.city
  u'Maynard'

  >>> stephan.birthday
  datetime.date(1980, 1, 25)

  >>> stephan.friends
  {u'roy': <Person Roy Mathew>}

  >>> stephan.phone
  <Phone +1-978-394-5124>

  >>> stephan.today
  datetime.datetime(2011, 10, 1, 9, 45)

  >>> stephan.visited
  [u'Germany', u'USA']


See src/mongopersist/README.txt and the other txt files in the package
for more details.

Travis: |buildstatus|_

.. |buildstatus| image:: https://api.travis-ci.org/zopefoundation/mongopersist.png?branch=master
.. _buildstatus: https://travis-ci.org/zopefoundation/mongopersist
