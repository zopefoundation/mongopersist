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
"""Mongo Mapping Implementations"""
import UserDict
import pymongo

from mongopersist import interfaces

class MongoCollectionMapping(UserDict.DictMixin, object):
    __mongo_database__ = None
    __mongo_collection__ = None
    __mongo_mapping_key__ = 'key'

    def __init__(self, jar):
        self._m_jar = jar

    def __mongo_filter__(self):
        return {}

    def get_mongo_collection(self):
        db_name = self.__mongo_database__ or self._m_jar.default_database
        return self._m_jar._conn[db_name][self.__mongo_collection__]

    def __getitem__(self, key):
        filter = self.__mongo_filter__()
        filter[self.__mongo_mapping_key__] = key
        doc = self.get_mongo_collection().find_one(filter)
        if doc is None:
            raise KeyError(key)
        db_name = self.__mongo_database__ or self._m_jar.default_database
        dbref = pymongo.dbref.DBRef(
            self.__mongo_collection__, doc['_id'], db_name)
        return self._m_jar._reader.get_ghost(dbref)

    def __setitem__(self, key, value):
        # Even though setting the attribute should register the object with
        # the data manager, the value might not be in the DB at all at this
        # point, so registering it manually ensures that new objects get added.
        self._m_jar.register(value)
        setattr(value, self.__mongo_mapping_key__, key)

    def __delitem__(self, key):
        # Deleting the object from the database is not our job. We simply
        # remove it from the dictionary.
        value = self[key]
        setattr(value, self.__mongo_mapping_key__, None)

    def keys(self):
        filter = self.__mongo_filter__()
        filter[self.__mongo_mapping_key__] = {'$ne': None}
        return [
            doc[self.__mongo_mapping_key__]
            for doc in self.get_mongo_collection().find(filter)]
