##############################################################################
#
# Copyright (c) 2012 Zope Foundation and Contributors.
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
"""Mongo Persistence Performance Test"""
from __future__ import absolute_import
import optparse
import persistent
import pymongo
import random
import sys
import time
import transaction
import cPickle
import cProfile

from mongopersist import conflict, datamanager
from mongopersist.zope import container

MULTIPLE_CLASSES = True


class People(container.AllItemsMongoContainer):
    _p_mongo_collection = 'people'
    _m_database = 'performance'
    _m_collection = 'person'

class Address(persistent.Persistent):
    _p_mongo_collection = 'address'

    def __init__(self, city):
        self.city = city

class Person(persistent.Persistent, container.MongoContained):
    _p_mongo_collection = 'person'
    _p_mongo_store_type = True

    def __init__(self, name, age):
        self.name = name
        self.age = age
        self.address = Address('Boston %i' %age)

    def __repr__(self):
        return '<%s %s @ %i [%s]>' %(
            self.__class__.__name__, self.name, self.age, self.__name__)

class Person2(Person):
    pass

def run_basic_crud(options):
    conn = pymongo.Connection('localhost', 27017, tz_aware=False)
    dm = datamanager.MongoDataManager(
        conn,
        default_database='performance',
        root_database='performance',
        conflict_handler_factory=conflict.ResolvingSerialConflictHandler)
    if options.reload:
        conn.drop_database('performance')
        dm.root['people'] = people = People()

        # Profile inserts
        transaction.begin()
        t1 = time.time()
        for idx in xrange(options.size):
            klass = Person if (MULTIPLE_CLASSES and idx % 2) else Person2
            people[None] = klass('Mr Number %.5i' %idx, random.randint(0, 100))
        transaction.commit()
        t2 = time.time()
        print 'Insert:       %.4f secs' % (t2-t1)

    else:
        people = dm.root['people']

    # Profile slow read
    transaction.begin()
    t1 = time.time()
    [people[name].name for name in people]
    #cProfile.runctx(
    #    '[people[name].name for name in people]', globals(), locals())
    t2 = time.time()
    transaction.commit()
    print 'Slow Read:        %.4f secs' % (t2-t1)

    # Profile fast read (values)
    transaction.begin()
    t1 = time.time()
    [person.name for person in people.values()]
    #cProfile.runctx(
    #    '[person.name for person in people.find()]', globals(), locals())
    t2 = time.time()
    transaction.commit()
    print 'Fast Read (values): %.4f secs' % (t2-t1)

    # Profile fast read
    transaction.begin()
    t1 = time.time()
    [person.name for person in people.find()]
    #cProfile.runctx(
    #    '[person.name for person in people.find()]', globals(), locals())
    t2 = time.time()
    transaction.commit()
    print 'Fast Read (find):   %.4f secs' % (t2-t1)

    if options.modify:
        # Profile modification
        t1 = time.time()
        def modify():
            for person in list(people.find()):
                person.name += 'X'
                person.age += 1
            transaction.commit()
        modify()
        #cProfile.runctx(
        #    'modify()', globals(), locals())
        t2 = time.time()
        print 'Modification:     %.4f secs' % (t2-t1)

    if options.delete:
        # Profile deletion
        t1 = time.time()
        for name in people.keys():
            del people[name]
        transaction.commit()
        t2 = time.time()
        print 'Deletion:         %.4f secs' % (t2-t1)

parser = optparse.OptionParser()
parser.usage = '%prog [options]'

parser.add_option(
    '-s', '--size', action='store', type='int',
    dest='size', default=1000,
    help='The amount of objects to use.')

parser.add_option(
    '--no-reload', action='store_false',
    dest='reload', default=True,
    help='A flag, when set, causes the DB not to be reloaded.')

parser.add_option(
    '--no-modify', action='store_false',
    dest='modify', default=True,
    help='A flag, when set, causes the data not to be modified.')

parser.add_option(
    '--no-delete', action='store_false',
    dest='delete', default=True,
    help='A flag, when set, causes the data not to be deleted at the end.')

def main(args=None):
    # Parse command line options.
    if args is None:
        args = sys.argv[1:]
    options, args = parser.parse_args(args)

    run_basic_crud(options)
