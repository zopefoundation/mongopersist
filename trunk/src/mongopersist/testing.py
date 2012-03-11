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
"""Mongo Persistence Testing Support"""
from __future__ import absolute_import
import doctest
import pymongo
import re
import transaction
from zope.testing import module, renormalizing

from mongopersist import datamanager, serialize

checker = renormalizing.RENormalizing([
    (re.compile(r'datetime.datetime(.*)'),
     'datetime.datetime(2011, 10, 1, 9, 45)'),
    (re.compile(r"ObjectId\('[0-9a-f]*'\)"),
     "ObjectId('4e7ddf12e138237403000000')"),
    (re.compile(r"object at 0x[0-9a-f]*>"),
     "object at 0x001122>"),
    ])

OPTIONFLAGS = (doctest.NORMALIZE_WHITESPACE|
               doctest.ELLIPSIS|
               doctest.REPORT_ONLY_FIRST_FAILURE
               #|doctest.REPORT_NDIFF
               )

def setUp(test):
    module.setUp(test)
    test.globs['conn'] = pymongo.Connection('localhost', 27017, tz_aware=False)
    test.globs['DBNAME'] = 'mongopersist_test'
    test.globs['conn'].drop_database(test.globs['DBNAME'])
    test.globs['commit'] = transaction.commit
    test.globs['dm'] = datamanager.MongoDataManager(
        test.globs['conn'],
        default_database=test.globs['DBNAME'],
        root_database=test.globs['DBNAME'])

def tearDown(test):
    module.tearDown(test)
    transaction.abort()
    test.globs['conn'].drop_database(test.globs['DBNAME'])
    test.globs['conn'].disconnect()
    serialize.SERIALIZERS.__init__()
