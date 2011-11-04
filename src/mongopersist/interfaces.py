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
"""Mongo Persistence Interfaces"""
from __future__ import absolute_import
import datetime
import persistent.interfaces
import transaction.interfaces
import types
import zope.interface
import zope.schema
from pymongo import objectid, dbref

MONGO_NATIVE_TYPES = (
    int, float, unicode, datetime.datetime, types.NoneType,
    objectid.ObjectId, dbref.DBRef)

class ConflictError(transaction.interfaces.TransientError):

    def __init__(self, message=None, object=None, serials=None):
        self.message = message or "database conflict error"
        self.object = object
        self.serials = serials

    @property
    def new_serial(self):
        return self.serials[0]

    @property
    def old_serial(self):
        return self.serials[1]

    def __str__(self):
        extras = [
            'oid %s' %self.object._p_oid,
            'class %s' %self.object.__class__.__name__,
            'start serial %s' %self.old_serial,
            'current serial %s' %self.new_serial]
        return "%s (%s)" % (self.message, ", ".join(extras))

    def __repr__(self):
        return '%s: %s' %(self.__class__.__name__, self)


class CircularReferenceError(Exception):
    pass

class IObjectSerializer(zope.interface.Interface):
    """An object serializer allows for custom serialization output for
    objects."""

    def can_read(state):
        """Returns a boolean indicating whether this serializer can deserialize
        this state."""

    def get_object(state):
        """Convert the state to an object."""

    def can_write(obj):
        """Returns a boolean indicating whether this serializer can serialize
        this object."""

    def get_state(obj):
        """Convert the object to a state/document."""


class IObjectWriter(zope.interface.Interface):
    """The object writer stores an object in the database."""

    def get_non_persistent_state(obj, seen):
        """Convert a non-persistent object to a Mongo state/document."""

    def get_persistent_state(obj, seen):
        """Convert a persistent object to a Mongo state/document."""

    def get_state(obj, seen=None):
        """Convert an arbitrary object to a Mongo state/document.

        A ``CircularReferenceError`` is raised, if a non-persistent loop is
        detected.
        """

    def store(obj):
        """Store an object in the database."""


class IObjectReader(zope.interface.Interface):
    """The object reader reads an object from the database."""

    def resolve(path):
        """Resolve a path to a class.

        The path can be any string. It is the responsibility of the resolver
        to maintain the mapping from path to class.
        """

    def get_object(state, obj):
        """Get an object from the given state.

        The ``obj`` is the Mongo document of which the created object is part
        of.
        """

    def set_ghost_state(obj):
        """Convert a ghosted object to an active object by loading its state.
        """

    def get_ghost(coll_name, oid):
        """Get the ghosted version of the object.
        """


class IMongoDataManager(persistent.interfaces.IPersistentDataManager):
    """A persistent data manager that stores data in Mongo."""

    root = zope.interface.Attribute(
        """Get the root object, which is a mapping.""")

    def reset():
        """Reset the datamanager for the next transaction."""

    def dump(obj):
        """Store the object to Mongo and return its DBRef."""

    def load(dbref):
        """Load the object from Mongo by using its DBRef.

        Note: The returned object is in the ghost state.
        """


class IMongoConnectionPool(zope.interface.Interface):
    """MongoDB connection pool"""

    connection = zope.interface.Attribute('MongoDBConnection instance')

    host = zope.schema.TextLine(
        title=u'MongoDB Server Hostname (without protocol)',
        description=u'MongoDB Server Hostname or IPv4 address',
        default=u'localhost',
        required=True)

    port = zope.schema.Int(
        title=u'MongoDB Server Port',
        description=u'MongoDB Server Port',
        default=27017,
        required=True)


class IMongoDataManagerProvider(zope.interface.Interface):
    """Utility to get a mongo data manager.

    Implementations of this utility ususally maintain connection information
    and ensure that there is one consistent datamanager per thread.
    """

    def get():
        """Return a mongo data manager."""
