# lshash/storage.py
# Copyright 2012 Kay Zhu (a.k.a He Zhu) and contributors (see CONTRIBUTORS.txt)
#
# This module is part of lshash and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

from __future__ import unicode_literals

import json
from collections import namedtuple
import hashlib
import pickle
try:
    import joblib
except ImportError:
    joblib = None
import sqlite3

try:
    import redis
except ImportError:
    redis = None


Levels = namedtuple("Levels", ["High", "Medium", "Low"])("high", "medium", "low")
_LEVELS_KEY_COEFFICIENTS = {
    Levels.High: 1.0,
    Levels.Medium: 0.75,
    Levels.Low: 0.5
}


__all__ = ['storage', 'serializer', 'BaseStorage', 'InMemoryStorage', 'RedisStorage', 'SQLiteStorage']

def storage(storage_config, index):
    """ Given the configuration for storage and the index, return the
    configured storage instance.
    """
    if 'dict' in storage_config:
        return InMemoryStorage(storage_config['dict'])
    elif 'redis' in storage_config:
        return RedisStorage(storage_config['redis'], index)
    elif "sqlite" in storage_config:
        return SQLiteStorage(storage_config['sqlite'], index)
    else:
        raise ValueError("Only in-memory dictionary and Redis are supported.")


def serializer(protocol=None):
    """ Given a protocole, return the corresponding serializer
    `protocol`: pickle | json
    """
    if protocol is None:
        protocol = "pickle"
    if protocol == "json":
        return json
    else:
        if joblib is not None:
            return joblib
        else:
            return pickle


def _compute_hash(message):
    if isinstance(message, bytes):
        raw_message = message
    else:
        raw_message = message.encode()
    return hashlib.sha1(raw_message).hexdigest()
    
class BaseStorage(object):
    def keys(self, level=None):
        """ Returns a list of binary hashes that are used as dict keys. """
        raise NotImplementedError

    def append_val(self, key, val):
        """ Append `val` to the list stored at `key`.

        If the key is not yet present in storage, create a list with `val` at
        `key`.
        """
        raise NotImplementedError

    def get_list(self, key, level=None):
        """ Returns a list stored in storage at `key`.

        This method should return a list of values stored at `key`. `[]` should
        be returned if the list is empty or if `key` is not present in storage.
        """
        raise NotImplementedError


class InMemoryStorage(BaseStorage):
    def __init__(self, h_index):
        self.name = 'dict'
        self.storage = dict()

    def keys(self, level=None):
        return self.storage.keys()

    def append_val(self, key, val):
        self.storage.setdefault(key, set()).update([val])

    def get_list(self, key, level=None):
        return list(self.storage.get(key, []))


class RedisStorage(BaseStorage):
    def __init__(self, config, h_index):
        if not redis:
            raise ImportError("redis-py is required to use Redis as storage.")
        self.name = 'redis'
        self.storage = redis.StrictRedis(**config)
        # a single db handles multiple hash tables, each one has prefix ``h[h_index].``
        self.h_index = 'h%.2i.' % int(h_index)

    def _list(self, key):
        return self.h_index + key

    def keys(self, pattern='*', level=None):
        # return the keys BUT be agnostic with reference to the hash table
        return [k.decode('ascii').split('.')[1] for k in self.storage.keys(self.h_index + pattern)]

    def append_val(self, key, val):
        self.storage.sadd(self._list(key), json.dumps(val))

    def get_list(self, key, level=None):
        _list = list(self.storage.smembers(self._list(key)))  # list elements are plain strings here
        _list = [json.loads(el.decode('ascii')) for el in _list]  # transform strings into python tuples
        for el in _list:
            # if len(el) is 2, then el[1] is the extra value associated to the element
            if len(el) == 2 and type(el[0]) == list:
                el[0] = tuple(el[0])
        _list = [tuple(el) for el in _list]
        return _list

class SQLiteStorage(BaseStorage):
    def __init__(self, config, h_index):
        """
        config:
            table: name of the database table, default: 'lshash'
            key_column: name of the column to hold the key, default: 'key'
            value_column: name of the column to hold the value, default: 'value'
            database: path to the database, default: ':memory:'
            serializer: 'json'|'pickle', default: 'pickle'
            enabled_levels: if True, add 2 more keys, which are derivated from the key for each item
        """
        super().__init__()
        self.name = "sqlite"
        self.config = {
            "table": "lshash",
            "key_column": "key",
            "value_column": "value",
            "database": ":memory:",
            "serializer": None,
            "enabled_levels": None
        }
        if config is None:
            self.config['serializer'] = serializer()
        else:
            self.config.update(config)
            self.config["serializer"] = serializer(config.get("serializer"))
        connection = sqlite3.connect(self.config["database"])
        self.config["connection"] = connection
        if h_index:
            table = f"{self.table}.{h_index}"
        else:
            table = self.table
        self._create_table(table, self.key_column, self.value_column, self.value_hash_column)

    def _create_table(self, table, key_column, value_column, value_hash_column):
        sql_indexes_create_statements = []
        if self.enabled_levels:
            index_key_column = self._get_level_key_column(Levels.High)
            key_fields_repr = ",".join(f"{self._get_level_key_column(level)} Text" for level in Levels)
            sql_create_table = f"CREATE TABLE IF NOT EXISTS {table} ({key_fields_repr}, {value_hash_column} Text, {value_column} Blob)"
            #sql_create_index_key = f"CREATE INDEX IF NOT EXISTS {table}_{index_key_column} ON {table}({index_key_column})"
            sql_create_index_value = f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_{value_hash_column} ON {table}({index_key_column}, {value_hash_column})"
            indexes_key_columns = [self._get_level_key_column(level) for level in Levels]
            sql_indexes_create_statements.extend([f"CREATE INDEX IF NOT EXISTS {table}_{index_key_column} ON {table}({index_key_column})" for index_key_column in indexes_key_columns])
            sql_indexes_create_statements.append(sql_create_index_value)
        else:
            sql_create_table = f"CREATE TABLE IF NOT EXISTS {table} ({key_column} Text, {value_hash_column} Text, {value_column} Blob)"
            sql_create_index_key = f"CREATE INDEX IF NOT EXISTS {table}_{key_column} ON {table}({key_column})"
            sql_create_index_value = f"CREATE UNIQUE INDEX IF NOT EXISTS {table}_{value_hash_column} ON {table}({key_column}, {value_hash_column})"
            sql_indexes_create_statements.extend([sql_create_index_key, sql_create_index_value])
        with self.connection as con:
            con.execute(sql_create_table)
            # con.execute(sql_create_index_key)
            # con.execute(sql_create_index_value)
            for sql_statement in sql_indexes_create_statements:
                con.execute(sql_statement)
    
    def _get_level_key_column(self, level):
        return f"{self.key_column}_{level}"
    
    def _get_level_key_value(self, key, level):
        size = int(len(key) * _LEVELS_KEY_COEFFICIENTS[level])
        if level == Levels.High:
            return key[:size]
        else:
            mixed_key = key[0::2] + key[1::2]
            return mixed_key[:size]
    
    def keys(self, level=Levels.High):
        if level is None:
            level = Levels.High
        if self.enabled_levels:
            level_key_column = self._get_level_key_column(level)
            sql = f"SELECT DISTINCT({level_key_column}) FROM {self.table}"
        else:
            sql = f"SELECT DISTINCT({self.key_column}) FROM {self.table}"
        raw_result = self.connection.execute(sql).fetchall()
        result = [item[0] for item in raw_result]
        return result

    def append_val(self, key, val):
        serialized_value = self.serializer.dumps(val)
        serialized_value_hash = _compute_hash(serialized_value)

        if self.enabled_levels:
            key_columns = [self._get_level_key_column(level) for level in Levels]
            key_columns_repr = ",".join(key_columns)
            key_columns_wildcards_repr = ",".join(["?"] * len(key_columns))
            sql = f"INSERT INTO {self.table} ({key_columns_repr}, {self.value_column}, {self.value_hash_column}) VALUES({key_columns_wildcards_repr}, ?, ?)"
            keys = [self._get_level_key_value(key, level) for level in Levels]
            params = [*keys, serialized_value, serialized_value_hash]
        else:
            sql = f"INSERT INTO {self.table} ({self.key_column}, {self.value_column}, {self.value_hash_column}) VALUES(?, ?, ?)"
            params = [key, serialized_value, serialized_value_hash]
        with self.connection as con:
            try:
                con.execute(sql, params)
            except sqlite3.IntegrityError:
                pass

    def get_list(self, key, level=None):
        if level is None:
            level = Levels.High
        if self.enabled_levels:
            level_key_column = self._get_level_key_column(level)
            sql = f"SELECT value FROM {self.table} WHERE {level_key_column} like ?"
        else:
            sql = f"SELECT value FROM {self.table} WHERE {self.key_column} like ?"
        with self.connection as con:
            raw_result = con.execute(sql, [key]).fetchall()
        result = [self.serializer.loads(value[0]) for value in raw_result]
        for el in raw_result:
            # if len(el) is 2, then el[1] is the extra value associated to the element
            if len(el) == 2 and type(el[0]) == list:
                el[0] = tuple(el[0])
        return [tuple(val) for val in result]
    
    @property
    def serializer(self):
        return self.config["serializer"]

    @property
    def connection(self) -> sqlite3.Connection:
        return self.config["connection"]

    @property
    def table(self) -> str:
        return self.config["table"]

    @property
    def key_column(self) -> str:
        return self.config["key_column"]

    @property
    def value_column(self) -> str:
        return self.config["value_column"]
    
    @property
    def value_hash_column(self) -> str:
        return f"{self.value_column}_hash"
    
    @property
    def enabled_levels(self) -> bool:
        return self.config["enabled_levels"]
