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
"""Mongo Persistence Conflict Handler Implementations"""
from __future__ import absolute_import
import struct
import zope.interface
from mongopersist import interfaces, serialize

def p64(v):
    """Pack an integer or long into a 8-byte string"""
    return struct.pack(">Q", v)

def u64(v):
    """Unpack an 8-byte string into a 64-bit long integer."""
    return struct.unpack(">Q", v)[0]

def create_conflict_error(obj, orig_doc, cur_doc, new_doc):
    return interfaces.ConflictError(None, obj, orig_doc, cur_doc, new_doc)

class NoCheckConflictHandler(object):
    zope.interface.implements(interfaces.IConflictHandler)

    def __init__(self, datamanager):
        self.datamanager = datamanager

    def on_before_set_state(self, obj, state):
        pass

    def on_before_store(self, obj, state):
        pass

    def on_after_store(self, obj, state):
        pass

    def on_modified(self, obj):
        pass

    def is_same(self, obj, orig_state, new_state):
        return orig_state == new_state

    def has_conflicts(self, objs):
        return False

    def check_conflicts(self, objs):
        pass


class SerialConflictHandler(object):
    zope.interface.implements(interfaces.IResolvingConflictHandler)

    field_name = '_py_serial'
    conflict_error_factory = staticmethod(create_conflict_error)

    def __init__(self, datamanager):
        self.datamanager = datamanager

    def on_before_set_state(self, obj, state):
        obj._p_serial = p64(state.pop(self.field_name, 0))

    def on_before_store(self, obj, state):
        state[self.field_name] = u64(getattr(obj, '_p_serial', 0)) + 1
        # Do not set the object serial yet, since we might not decide to store
        # after all.

    def on_after_store(self, obj, state):
        obj._p_serial = p64(state[self.field_name])

    def on_modified(self, obj):
        pass

    def is_same(self, obj, orig_state, new_state):
        if orig_state is None:
            # This should never happen in a real running system.
            return False
        orig_state = orig_state.copy()
        try:
            orig_state.pop(self.field_name)
        except KeyError:
            pass
        new_state = new_state.copy()
        try:
            new_state.pop(self.field_name)
        except KeyError:
            pass
        return orig_state == new_state

    def resolve(self, obj, orig_doc, cur_doc, new_doc):
        raise NotImplementedError

    def check_conflict(self, obj):
        # This object is not even added to the database yet, so there
        # cannot be a conflict.
        if obj._p_oid is None:
            return
        coll = self.datamanager._get_collection_from_object(obj)
        cur_doc = coll.find_one(obj._p_oid.id, fields=(self.field_name,))
        if cur_doc is None:
            return
        if cur_doc.get(self.field_name, 0) != u64(obj._p_serial):
            orig_doc = self.datamanager._original_states.get(obj._p_oid)
            cur_doc = coll.find_one(obj._p_oid.id)
            new_doc = self.datamanager._writer.get_full_state(obj)
            resolved = self.resolve(obj, orig_doc, cur_doc, new_doc)
            if not resolved:
                return self.conflict_error_factory(
                    obj, orig_doc, cur_doc, new_doc)

    def has_conflicts(self, objs):
        for obj in objs:
            if self.check_conflict(obj) is not None:
                return True
        return False

    def check_conflicts(self, objs):
        for obj in objs:
            err = self.check_conflict(obj)
            if err is not None:
                raise err


class SimpleSerialConflictHandler(SerialConflictHandler):

    def resolve(self, obj, orig_doc, cur_doc, new_doc):
        return False


class ResolvingSerialConflictHandler(SerialConflictHandler):

    def resolve(self, obj, orig_doc, cur_doc, new_doc):
        if hasattr(obj, '_p_resolveConflict'):
            doc = obj._p_resolveConflict(orig_doc, cur_doc, new_doc)
            if doc is not None:
                doc[self.field_name] = cur_doc[self.field_name]
                self.datamanager._reader.set_ghost_state(obj, doc)
                return True
        return False
