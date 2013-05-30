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
"""Zope Dublin Core Mongo Backend Storage"""
from UserDict import DictMixin

import zope.interface
from zope.location import Location
from zope.dublincore.interfaces import IWriteZopeDublinCore
from zope.dublincore.zopedublincore import ZopeDublinCore
from zope.security.proxy import removeSecurityProxy

class DCDataWrapper(DictMixin):

    def __init__(self, data):
        self.data = data

    def __getitem__(self, key):
        return self.data[key.replace('.', '_')]

    def __setitem__(self, key, value):
        self.data[key.replace('.', '_')] = value

    def __delitem__(self, key):
        del self.data[key.replace('.', '_')]

    def keys(self):
        return [k.replace('_', '.') for k in self.data.keys()]


@zope.interface.implementer(IWriteZopeDublinCore)
class ZDCAnnotatableAdapter(ZopeDublinCore, Location):
    """Adapt annotatable objects to Zope Dublin Core."""
    DCKEY = 'dc'

    def __init__(self, context):
        self.__parent__ = context
        self.__name__ = self.DCKEY
        naked = removeSecurityProxy(context)
        if not hasattr(naked, self.__name__):
            setattr(naked, self.__name__, {})
        dcdata = DCDataWrapper(getattr(naked, self.__name__))
        super(ZDCAnnotatableAdapter, self).__init__(dcdata)

    def _changed(self):
        self.__parent__._p_changed = True
