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
from bson import objectid, dbref

MONGO_NATIVE_TYPES = (
    int, float, unicode, datetime.datetime, types.NoneType,
    objectid.ObjectId, dbref.DBRef)


class ConflictError(transaction.interfaces.TransientError):
    """An error raised when a write conflict is detected."""

    def __init__(self, message=None, object=None,
                 orig_state=None, cur_state=None, new_state=None):
        self.message = message or "database conflict error"
        self.object = object
        self.orig_state = orig_state
        self.cur_state = cur_state
        self.new_state = new_state

    @property
    def orig_serial(self):
        return self.orig_state.get('_py_serial') if self.orig_state else None

    @property
    def cur_serial(self):
        return self.cur_state.get('_py_serial') if self.cur_state else None

    @property
    def new_serial(self):
        return self.new_state.get('_py_serial') if self.new_state else None

    def __str__(self):
        extras = [
            'oid %s' % self.object._p_oid,
            'class %s' % self.object.__class__.__name__,
            'orig serial %s' % self.orig_serial,
            'cur serial %s' % self.cur_serial,
            'new serial %s' % self.new_serial]
        return "%s (%s)" % (self.message, ", ".join(extras))

    def __repr__(self):
        return '%s: %s' % (self.__class__.__name__, self)


class CircularReferenceError(Exception):
    pass


class IConflictHandler(zope.interface.Interface):

    datamanager = zope.interface.Attribute(
        """The datamanager for which to conduct the conflict resolution.""")

    def on_before_set_state(obj, state):
        """Method called just before the object's state is set."""

    def on_before_store(obj, state):
        """Method called just before the object state is written to MongoDB."""

    def on_after_store(obj, state):
        """Method called right after the object state was written to MongoDB."""

    def on_modified(obj):
        """Method called when an object is registered as modified."""

    def is_same(obj, orig_state, new_state):
        """Compares two states of the object and determines whether they are
        the same. It should only compare actual object fields and not any
        meta-data fields."""

    def has_conflicts(objs):
        """Checks whether any of the passed in objects have conflicts.

        Returns False if conflicts were found, otherwise True is returned.

        While calling this method, the conflict handler may try to resolve
        conflicts.
        """

    def check_conflicts(self, objs):
        """Checks whether any of the passed in objects have conflicts.

        Raises a ``ConflictError`` for the first object with a conflict.

        While calling this method, the conflict handler may try to resolve
        conflicts.
        """


class IResolvingConflictHandler(IConflictHandler):
    """A conflict handler that is able to resolve conflicts."""

    def resolve(obj, orig_doc, cur_doc, new_doc):
        """Tries to resolve a conflict.

        This is usually done through some comparison of the states. The method
        returns ``True`` if the conflict was resolved and ``False`` otherwise.

        It is the responsibility of this method to modify the object and data
        manager models, so that the resolution is valid in the next step.
        """


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

    conflict_handler = zope.interface.Attribute(
        """An ``IConflictHandler`` instance that handles all conflicts.""")

    def get_collection(db_name, coll_name):
        """Return the collection for the given DB and collection names."""

    def get_collection_of_object(obj):
        """Return the collection for an object."""

    def reset():
        """Reset the datamanager for the next transaction."""

    def dump(obj):
        """Store the object to Mongo and return its DBRef."""

    def load(dbref):
        """Load the object from Mongo by using its DBRef.

        Note: The returned object is in the ghost state.
        """

    def flush():
        """Flush all changes to Mongo."""

    def insert(obj):
        """Insert an object into Mongo.

        The correct collection is determined by object type.
        """

    def remove(obj):
        """Remove an object from Mongo.

        The correct collection is determined by object type.
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


class IMongoSpecProcessor(zope.interface.Interface):
    """An adapter to process find/update spec's"""

    def process(collection, spec):
        """return the processed spec here"""


class IMongoContainer(zope.interface.Interface):
    _m_database = zope.schema.ASCIILine(
        title=u'Mongo Database',
        description=(
            u'Specifies the Mongo DB in which to store items. If ``None``, the '
            u'default database will be used.'),
        default=None)

    _m_collection = zope.schema.ASCIILine(
        title=u'Mongo Collection',
        description=(
            u'Specifies the Mongo collection in which to store items.')
        )

    _m_mapping_key = zope.schema.ASCIILine(
        title=u'Mapping Key',
        description=(
            u'Specifies the attribute name of the item that is used as the '
            u'mapping/dictionary/container key.'),
        default='key')

    _m_parent_key = zope.schema.ASCIILine(
        title=u'Parent Key',
        description=(
            u'Specifies the attribute name of the item that is used to store '
            u'the parent/container reference.'),
        default='parent')

    _m_remove_documents = zope.schema.Bool(
        title=u'Remove Documents',
        description=(
            u'A flag when set causes documents to be removed from the DB when '
            u'they are removed from the container.'),
        default=True)

    def _m_get_parent_key_value():
        """Returns the value that is used to specify a particular container as
        the parent of the item.
        """

    def _m_get_items_filter():
        """Returns a query spec representing a filter that only returns
        objects in this container."""

    def _m_add_items_filter(filter):
        """Applies the item filter items to the provided filter.

        Keys that are already in the passed in filter are not overwritten.
        """

    def get_collection():
        """Get the Python representation of the collection.

        This can be useful to make custom queries against the collection.
        """

    def raw_find(spec=None, *args, **kwargs):
        """Return a raw Mongo result set for the specified query.

        The spec is updated to also contain the container's filter spec.

        See pymongo's documentation for details on *args and **kwargs.
        """

    def find(spec=None, fields=None, *args, **kwargs):
        """Return a Python object result set for the specified query.

        By default only the Mongo Id and key attribute is requested and a
        ghost is created. The rest of the data is only retrieved if needed.

        The spec is updated to also contain the container's filter spec.

        See pymongo's documentation for details on *args and **kwargs.
        """

    def raw_find_one(spec_or_id=None, *args, **kwargs):
        """Return a raw Mongo document for the specified query.

        The spec is updated to also contain the container's filter spec.

        See pymongo's documentation for details on *args and **kwargs.
        """

    def find_one(spec_or_id=None, fields=None, *args, **kwargs):
        """Return a single Python object for the specified query.

        The spec is updated to also contain the container's filter spec.

        See pymongo's documentation for details on *args and **kwargs.
        """

    def add(value, key=None):
        """Add an object without necessarily knowing the key of the object.

        Either pass in a valid key or the key will be:
        - in case _m_mapping_key is None: the object's OID
        - otherwise getattr(value, _m_mapping_key)
        """
