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
import os
import persistent
import pymongo
import random
import sys
import tempfile
import time
import transaction
import cPickle
import cProfile

from mongopersist import conflict, datamanager
from mongopersist.zope import container

import zope.container
import zope.container.btree
import ZODB
import ZODB.FileStorage


MULTIPLE_CLASSES = True


class People(container.AllItemsMongoContainer):
    _p_mongo_collection = 'people'
    _m_database = 'performance'
    _m_collection = 'person'
    _m_mapping_key = 'name'

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


class PerformanceBase(object):
    personKlass = None
    person2Klass = None

    def printResult(self, text, t1, t2, count=None):
        dur = t2-t1
        text += ':'
        ops = ''
        if count:
            ops = "%d ops/second" % (count / dur)

        print '%-25s %.4f secs %s' % (text, dur, ops)

    def getPeople(self, options):
        pass

    def slow_read(self, people, peopleCnt):
        # Profile slow read
        transaction.begin()
        t1 = time.time()
        [people[name].name for name in people]
        #cProfile.runctx(
        #    '[people[name].name for name in people]', globals(), locals())
        t2 = time.time()
        transaction.commit()
        self.printResult('Slow Read', t1, t2, peopleCnt)

    def fast_read_values(self, people, peopleCnt):
        # Profile fast read (values)
        transaction.begin()
        t1 = time.time()
        [person.name for person in people.values()]
        #cProfile.runctx(
        #    '[person.name for person in people.find()]', globals(), locals())
        t2 = time.time()
        transaction.commit()
        self.printResult('Fast Read (values)', t1, t2, peopleCnt)

    def fast_read(self, people, peopleCnt):
        # Profile fast read
        transaction.begin()
        t1 = time.time()
        [person.name for person in people.find()]
        #cProfile.runctx(
        #    '[person.name for person in people.find()]', globals(), locals())
        t2 = time.time()
        transaction.commit()
        self.printResult('Fast Read (find)', t1, t2, peopleCnt)

    def object_caching(self, people, peopleCnt):
        # Profile object caching
        transaction.begin()
        t1 = time.time()
        [person.name for person in people.values()]
        [person.name for person in people.values()]
        #cProfile.runctx(
        #    '[person.name for person in people.values()]', globals(), locals())
        t2 = time.time()
        transaction.commit()
        self.printResult('Fast Read (caching x2)', t1, t2, peopleCnt*2)

        transaction.begin()
        t1 = time.time()
        [person.name for person in people.values()]
        [person.name for person in people.values()]
        [person.name for person in people.values()]
        #cProfile.runctx(
        #    '[person.name for person in people.values()]', globals(), locals())
        t2 = time.time()
        transaction.commit()
        self.printResult('Fast Read (caching x3)', t1, t2, peopleCnt*3)

        transaction.begin()
        t1 = time.time()
        [person.name for person in people.values()]
        [person.name for person in people.values()]
        [person.name for person in people.values()]
        [person.name for person in people.values()]
        #cProfile.runctx(
        #    '[person.name for person in people.values()]', globals(), locals())
        t2 = time.time()
        transaction.commit()
        self.printResult('Fast Read (caching x4)', t1, t2, peopleCnt*4)

    def modify(self, people, peopleCnt):
        # Profile modification
        t1 = time.time()
        def modify():
            for person in list(people.values()):
                person.name += 'X'
                person.age += 1
            transaction.commit()
        modify()
        #cProfile.runctx(
        #    'modify()', globals(), locals())
        t2 = time.time()
        self.printResult('Modification', t1, t2, peopleCnt)

    def delete(self, people, peopleCnt):
        # Profile deletion
        t1 = time.time()
        for name in people.keys():
            del people[name]
        transaction.commit()
        t2 = time.time()
        self.printResult('Deletion', t1, t2, peopleCnt)

    def run_basic_crud(self, options):
        people = self.getPeople(options)

        peopleCnt = len(people)

        self.slow_read(people, peopleCnt)
        self.fast_read_values(people, peopleCnt)
        self.fast_read(people, peopleCnt)
        self.object_caching(people, peopleCnt)

        if options.modify:
            self.modify(people, peopleCnt)

        if options.delete:
            self.delete(people, peopleCnt)


class PerformanceMongo(PerformanceBase):
    personKlass = Person
    person2Klass = Person2

    def getPeople(self, options):
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
                klass = (self.personKlass if (MULTIPLE_CLASSES and idx % 2)
                         else self.person2Klass)
                people[None] = klass('Mr Number %.5i' % idx,
                                     random.randint(0, 100))
            transaction.commit()
            t2 = time.time()
            self.printResult('Insert', t1, t2, options.size)
        else:
            people = dm.root['people']

        return people


class PeopleZ(zope.container.btree.BTreeContainer):
    pass

class AddressZ(persistent.Persistent):

    def __init__(self, city):
        self.city = city

class PersonZ(persistent.Persistent, zope.container.contained.Contained):

    def __init__(self, name, age):
        self.name = name
        self.age = age
        self.address = AddressZ('Boston %i' %age)

    def __repr__(self):
        return '<%s %s @ %i [%s]>' %(
            self.__class__.__name__, self.name, self.age, self.__name__)

class Person2Z(Person):
    pass


class PerformanceZODB(PerformanceBase):
    personKlass = PersonZ
    person2Klass = Person2Z

    def getPeople(self, options):
        folder = tempfile.gettempdir()
        #folder = './'  # my /tmp is a tmpfs
        fname = os.path.join(folder, 'performance_data.fs')
        if options.reload:
            try:
                os.remove(fname)
            except:
                pass
        fs = ZODB.FileStorage.FileStorage(fname)
        db = ZODB.DB(fs)
        conn = db.open()

        root = conn.root()

        if options.reload:
            root['people'] = people = PeopleZ()
            transaction.commit()

            # Profile inserts
            transaction.begin()
            t1 = time.time()
            for idx in xrange(options.size):
                klass = (self.personKlass if (MULTIPLE_CLASSES and idx % 2)
                         else self.person2Klass)
                name = 'Mr Number %.5i' % idx
                people[name] = klass(name, random.randint(0, 100))
            transaction.commit()
            t2 = time.time()
            self.printResult('Insert', t1, t2, options.size)
        else:
            people = root['people']

        return people

    def fast_read(self, people, peopleCnt):
        pass


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

    print 'MONGO ---------------'
    PerformanceMongo().run_basic_crud(options)
    print 'ZODB  ---------------'
    PerformanceZODB().run_basic_crud(options)
