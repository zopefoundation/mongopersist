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
"""Thread-aware Mongo Connection Pool"""
from __future__ import absolute_import
import logging
import threading
import pymongo
import zope.interface

from mongopersist import datamanager, interfaces

log = logging.getLogger('mongopersist')

class MongoConnectionPool(object):
    """MongoDB connection pool contains the connection to a mongodb server.

    MongoConnectionPool is a global named utility, knows how to setup a
    thread (safe) shared mongodb connection instance.

    Note: pymongo offers connection pooling which we do not need since we use
    one connection per thread
    """
    zope.interface.implements(interfaces.IMongoConnectionPool)

    _mongoConnectionFactory = pymongo.Connection

    def __init__(self, host='localhost', port=27017, logLevel=20,
        tz_aware=True, connectionFactory=None):
        self.host = host
        self.port = port
        self.key = 'mongopersist-%s-%s' %(self.host, self.port)
        self.tz_aware = tz_aware
        if connectionFactory is not None:
            self._mongoConnectionFactory = connectionFactory
        self.logLevel = logLevel

    @property
    def storage(self):
        return LOCAL.__dict__

    def disconnect(self):
        conn = self.storage.get(self.key, None)
        if conn is not None:
            conn.disconnect()
        self.storage[self.key] = None

    @property
    def connection(self):
        conn = self.storage.get(self.key, None)
        if conn is None:
            self.storage[self.key] = conn = self._mongoConnectionFactory(
                self.host, self.port, tz_aware=self.tz_aware)
            if self.logLevel:
                log.log(self.logLevel, "Create connection for %s:%s" % (
                    self.host, self.port))

        return conn


LOCAL = threading.local()

class MongoDataManagerProvider(object):
    zope.interface.implements(interfaces.IMongoDataManagerProvider)

    def __init__(self, host='localhost', port=27017,
                 logLevel=20, tz_aware=True, **dm_kwargs):
        self.pool = MongoConnectionPool(host, port, logLevel, tz_aware)
        self.dm_kwargs = dm_kwargs

    def get(self):
        try:
            dm = LOCAL.data_manager
        except AttributeError, err:
            conn = self.pool.connection
            dm = LOCAL.data_manager = datamanager.MongoDataManager(
                conn, **self.dm_kwargs)
        return dm
