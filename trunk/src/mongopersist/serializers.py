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
"""Python Serializers for common objects with weird reduce output."""
import datetime
from mongopersist import serialize

class DateSerializer(serialize.ObjectSerializer):

    def can_read(self, state):
        return isinstance(state, dict) and \
               state.get('_py_type') == 'datetime.date'

    def read(self, state):
        return datetime.date.fromordinal(state['ordinal'])

    def can_write(self, obj):
        return isinstance(obj, datetime.date)

    def write(self, obj):
        return {'_py_type': 'datetime.date',
                'ordinal': obj.toordinal()}
