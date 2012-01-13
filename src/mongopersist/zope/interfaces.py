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
"""Mongo Persistence Zope Container Interfaces"""
import zope.interface
import zope.schema


class IMongoContainer(zope.interface.Interface):
    _m_database = zope.schema.ASCIILine(
        title=u'Mongo Database',
        description=(
            u'Specifies the MDB in which to store items. If ``None``, the '
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

    def _m_get_parent_key_value():
        """Returns the value that is used to specify a particular container as
        the parent of the item.
        """

    def _m_get_items_filter():
        """Returns a query spec representing a filter that only returns
        objects in this container."""

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
