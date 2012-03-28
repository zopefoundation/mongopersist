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

from zope.exceptions import exceptionformatter

from mongopersist import testing

def setUp(test):
    testing.setUp(test)
    # silence this, otherwise half-baked objects raise exceptions
    # on trying to __repr__ missing attributes
    test.orig_DEBUG_EXCEPTION_FORMATTER = exceptionformatter.DEBUG_EXCEPTION_FORMATTER
    exceptionformatter.DEBUG_EXCEPTION_FORMATTER = 0

def tearDown(test):
    testing.tearDown(test)
    exceptionformatter.DEBUG_EXCEPTION_FORMATTER = test.orig_DEBUG_EXCEPTION_FORMATTER

def test_suite():
    return doctest.DocFileSuite(
        '../README.txt',
        setUp=setUp, tearDown=tearDown,
        checker=testing.checker,
        optionflags=testing.OPTIONFLAGS
        )
