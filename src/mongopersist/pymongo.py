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
"""PyMongo Patches"""
from __future__ import absolute_import
from bson.son import SON
from copy import deepcopy

def DBRef__init__(self, collection, id, database=None, _extra=None):
    self._DBRef__collection = collection
    self._DBRef__id = id
    self._DBRef__database = database
    self._DBRef__kwargs = {}
    self._hash = None

def DBRef__hash__(self):
    if self._hash is None:
        self._hash = hash(
            (self._DBRef__collection, self._DBRef__id, self._DBRef__database))
    return self._hash

def patch():
    # ObjectId should get patched too, but it is hard, since it uses slots
    # *and* rquires the original object reference to be around (otherwise it
    # creates BSON encoding errors.
    import bson.dbref
    bson.dbref.DBRef.__init__ = DBRef__init__
    bson.dbref.DBRef.__hash__ = DBRef__hash__
    del bson.dbref.DBRef.__getattr__
    del bson.dbref.DBRef.__setstate__
