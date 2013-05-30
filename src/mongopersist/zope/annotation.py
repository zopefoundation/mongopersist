##############################################################################
#
# Copyright (c) 2013 Zope Foundation and Contributors.
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
"""Mongo Annotations Implementation."""
from persistent.dict import PersistentDict
from zope import component, interface
from zope.annotation import interfaces

try:
    from UserDict import DictMixin
except ImportError:
    from collections import MutableMapping as DictMixin

class IMongoAttributeAnnotatable(interfaces.IAnnotatable):
    """Marker indicating that annotations can be stored on an attribute.

    This is a marker interface giving permission for an `IAnnotations`
    adapter to store data in an attribute named `__annotations__`.

    """

def normalize_key(key):
    return key.replace('.', '_')

@interface.implementer(interfaces.IAnnotations)
@component.adapter(IMongoAttributeAnnotatable)
class AttributeAnnotations(DictMixin):
    """Store annotations on an object

    Store annotations in the `__annotations__` attribute on a
    `IAttributeAnnotatable` object.
    """

    def __init__(self, obj, context=None):
        self.obj = obj

    def __bool__(self):
        return True

    __nonzero__ = __bool__

    def get(self, key, default=None):
        """See zope.annotation.interfaces.IAnnotations"""
        key = normalize_key(key)
        return getattr(self.obj, key, default)

    def __getitem__(self, key):
        key = normalize_key(key)
        try:
            return getattr(self.obj, key)
        except AttributeError:
            raise KeyError(key)

    def keys(self):
        annotations = getattr(self.obj, self.ATTR_NAME, None)
        if annotations is None:
            return []

        return annotations.keys()

    def __iter__(self):
        annotations = getattr(self.obj, self.ATTR_NAME, None)
        if annotations is None:
            return iter([])

        return iter(annotations)

    def __len__(self):
        raise NotImplementedError

    def __setitem__(self, key, value):
        """See zope.annotation.interfaces.IAnnotations"""
        key = normalize_key(key)
        setattr(self.obj, key, value)

    def __delitem__(self, key):
        """See zope.app.interfaces.annotation.IAnnotations"""
        key = normalize_key(key)
        try:
            delattr(self.obj, key)
        except AttributeError:
            raise KeyError(key)
