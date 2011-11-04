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

from mongopersist import testing, mapping

class Item(persistent.Persistent):
    def __init__(self, name=None, site=None):
        self.name = name
        self.site = site

def doctest_MongoCollectionMapping_simple():
    r"""MongoCollectionMapping: simple

    The Mongo Collection Mapping provides a Python dict interface for a mongo
    collection. Here is a simple example for our Item class/collection:

      >>> class SimpleContainer(mapping.MongoCollectionMapping):
      ...     __mongo_collection__ = 'mongopersist.tests.test_mapping.Item'
      ...     __mongo_mapping_key__ = 'name'

    To initialize the mapping, we need a data manager:

      >>> container = SimpleContainer(dm)

    Let's do some obvious initial manipulations:

      >>> container['one'] = one = Item()
      >>> one.name
      'one'
      >>> transaction.commit()

    After the transaction is committed, we can access the item:

      >>> container.keys()
      [u'one']
      >>> container['one'].name
      u'one'

      >>> container['two']
      Traceback (most recent call last):
      ...
      KeyError: 'two'

    Of course we can delete an item, but note that it only removes the name,
    but does not delete the document by default:

      >>> del container['one']
      >>> transaction.commit()
      >>> container.keys()
      []

    Finally, you can always get to the collection that the mapping is
    managing:

      >>> container.get_mongo_collection()
      Collection(Database(Connection('localhost', 27017),
                          u'mongopersist_test'),
                          u'mongopersist.tests.test_mapping.Item')
    """

def doctest_MongoCollectionMapping_filter():
    r"""MongoCollectionMapping: filter

    It is often desirable to manage multiple mappings for the same type of
    object and thus same collection. The mongo mapping thus supports filtering
    for all its functions.

      >>> class SiteContainer(mapping.MongoCollectionMapping):
      ...     __mongo_collection__ = 'mongopersist.tests.test_mapping.Item'
      ...     __mongo_mapping_key__ = 'name'
      ...     def __init__(self, jar, site):
      ...         super(SiteContainer, self).__init__(jar)
      ...         self.site = site
      ...     def __mongo_filter__(self):
      ...         return {'site': self.site}

      >>> container1 = SiteContainer(dm, 'site1')
      >>> container2 = SiteContainer(dm, 'site2')

    Let's now add some items:

      >>> ref11 = dm.dump(Item('1-1', 'site1'))
      >>> ref12 = dm.dump(Item('1-2', 'site1'))
      >>> ref13 = dm.dump(Item('1-3', 'site1'))
      >>> ref21 = dm.dump(Item('2-1', 'site2'))

    And accessing the items works as expected:

      >>> dm.reset()
      >>> container1.keys()
      [u'1-1', u'1-2', u'1-3']
      >>> container1['1-1'].name
      u'1-1'
      >>> container1['2-1']
      Traceback (most recent call last):
      ...
      KeyError: '2-1'

      >>> container2.keys()
      [u'2-1']

    Note: The mutator methods (``__setitem__`` and ``__delitem__``) do nto
    take the filter into account by default. They need to be extended to
    properly setup and tear down the filter criteria.
    """

def test_suite():
    return doctest.DocTestSuite(
        setUp=testing.setUp, tearDown=testing.tearDown,
        checker=testing.checker,
        optionflags=testing.OPTIONFLAGS)
