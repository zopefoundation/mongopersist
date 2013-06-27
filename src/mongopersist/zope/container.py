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
"""Mongo Persistence Zope Containers"""
import UserDict
import persistent
import bson.dbref
import bson.objectid
import zope.component
from bson.errors import InvalidId
from rwproperty import getproperty, setproperty
from zope.container import contained, sample
from zope.container.interfaces import IContainer

import mongopersist.container
from mongopersist import interfaces
from mongopersist.zope import interfaces as zinterfaces


class MongoContained(contained.Contained, mongopersist.container.MongoContained):
    pass  # just mix in the zope Contained magic


class SimpleMongoContainer(sample.SampleContainer, persistent.Persistent):
    _m_remove_documents = True

    def __getstate__(self):
        state = super(SimpleMongoContainer, self).__getstate__()
        state['data'] = state.pop('_SampleContainer__data')
        return state

    def __setstate__(self, state):
        # Mongopersist always reads a dictionary as persistent dictionary. And
        # modifying this dictionary will cause the persistence mechanism to
        # kick in. So we create a new object that we can easily modify without
        # harm.
        state = dict(state)
        state['_SampleContainer__data'] = state.pop('data', {})
        super(SimpleMongoContainer, self).__setstate__(state)

    def __getitem__(self, key):
        obj = super(SimpleMongoContainer, self).__getitem__(key)
        obj._v_name = key
        obj._v_parent = self
        return obj

    def get(self, key, default=None):
        '''See interface `IReadContainer`'''
        obj = super(SimpleMongoContainer, self).get(key, default)
        if obj is not default:
            obj._v_name = key
            obj._v_parent = self
        return obj

    def items(self):
        items = super(SimpleMongoContainer, self).items()
        for key, obj in items:
            obj._v_name = key
            obj._v_parent = self
        return items

    def values(self):
        return [v for k, v in self.items()]

    def __setitem__(self, key, obj):
        super(SimpleMongoContainer, self).__setitem__(key, obj)
        self._p_changed = True

    def __delitem__(self, key):
        obj = self[key]
        super(SimpleMongoContainer, self).__delitem__(key)
        if self._m_remove_documents:
            self._p_jar.remove(obj)
        self._p_changed = True


class MongoContainer(contained.Contained,
                     mongopersist.container.MongoContainer):
    zope.interface.implements(IContainer)

    def _after_setitem_hook(self, key, value):
        # We want to be as close as possible to using the Zope semantics.
        contained.setitem(self, self._real_setitem, key, value)

    def _after_delitem_hook(self, key, value):
        # Send the uncontained event.
        contained.uncontained(value, self, key)


class IdNamesMongoContainer(MongoContainer):
    """A container that uses the Mongo ObjectId as the name/key."""
    _m_mapping_key = None

    def __init__(self, collection=None, database=None, parent_key=None):
        super(IdNamesMongoContainer, self).__init__(collection, database, parent_key)

    @property
    def _m_remove_documents(self):
        # Objects must be removed, since removing the _id of a document is not
        # allowed.
        return True

    def _locate(self, obj, doc):
        obj._v_name = unicode(doc['_id'])
        obj._v_parent = self

    def __getitem__(self, key):
        try:
            id = bson.objectid.ObjectId(key)
        except InvalidId:
            raise KeyError(key)
        filter = self._m_get_items_filter()
        filter['_id'] = id
        obj = self.find_one(filter)
        if obj is None:
            raise KeyError(key)
        return obj

    def __contains__(self, key):
        try:
            id = bson.objectid.ObjectId(key)
        except InvalidId:
            return False
        return self.raw_find_one({'_id': id}, fields=()) is not None

    def __iter__(self):
        result = self.raw_find(fields=None)
        for doc in result:
            yield unicode(doc['_id'])

    def iteritems(self):
        result = self.raw_find()
        for doc in result:
            obj = self._load_one(doc)
            yield unicode(doc['_id']), obj


class AllItemsMongoContainer(MongoContainer):
    _m_parent_key = None


class SubDocumentMongoContainer(MongoContained, MongoContainer):
    _p_mongo_sub_object = True
