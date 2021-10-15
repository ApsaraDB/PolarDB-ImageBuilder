#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# Copyright (c) 2021, Alibaba Group Holding Limited
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#

"""
This is the pg connection functions
"""

import time

import psycopg2

from pg_utils.pg_const import SYSTEM_ACCOUNT_AURORA, RDS_INTERNAL_MARK


class Connection(object):
    """A lightweight wrapper around psycopg2 DB-API connections.

    The main value we provide is wrapping rows in a dict/object so that
    columns can be accessed by name. Typical usage::

        db = pg_database.Connection("127.0.0.1", 3422, "user", "password", "mydatabase")
        for article in db.query("SELECT * FROM articles"):
            print article.title
        db.close()
    """

    def __init__(
        self,
        host="",
        port="0",
        user="NON_EXIST_USER",
        password="",
        database="postgres",
        max_idle_time=600,
        connect_timeout=3,
        autocommit=True,
        options="-c DateStyle=ISO",
        **kwargs
    ):
        self.host = host
        self.port = port
        self.database = database
        self.max_idle_time = float(max_idle_time)
        self.autocommit = autocommit
        self.options = options

        if host == "":
            args = dict(
                port=port,
                dbname=database,
                user=user,
                password=password,
                connect_timeout=connect_timeout,
                options=options,
                **kwargs
            )
        else:
            args = dict(
                host=host,
                port=port,
                dbname=database,
                user=user,
                password=password,
                connect_timeout=connect_timeout,
                options=options,
                **kwargs
            )
        self._db = None
        self._db_args = args
        self._last_use_time = time.time()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def close(self):
        """Closes this database connection."""
        if getattr(self, "_db", None) is not None:
            self._db.close()
            self._db = None

    def reconnect(self):
        """Closes the existing database connection and re-opens it."""
        self.close()
        self._db = psycopg2.connect(**self._db_args)
        self._db.autocommit = self.autocommit

    def iter(self, query, *parameters, **kwparameters):
        """Returns an iterator for the given query and parameters."""
        self._ensure_connected()
        cursor = psycopg2.cursors.SSCursor(self._db)
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            for row in cursor:
                yield Row(zip(column_names, row))
        finally:
            cursor.close()

    def query(self, query, *parameters, **kwparameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(zip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def execute(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        return self.execute_lastrowid(query, *parameters, **kwparameters)

    def execute_lastrowid(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the lastrowid from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def execute_rowcount(self, query, *parameters, **kwparameters):
        """Executes the given query, returning the rowcount from the query."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters, kwparameters)
            return cursor.rowcount
        finally:
            cursor.close()

    update = execute_rowcount
    insert = execute_lastrowid

    def _ensure_connected(self):
        if self._db is None or (time.time() - self._last_use_time > self.max_idle_time):
            self.reconnect()
        self._last_use_time = time.time()

    def _cursor(self):
        self._ensure_connected()
        return self._db.cursor()

    def _execute(self, cursor, query, parameters, kwparameters):
        try:
            query = RDS_INTERNAL_MARK + query
            return cursor.execute(query, kwparameters or parameters)
        except psycopg2.OperationalError as e:
            self.close()
            # raise real exception to debug
            raise e


class Row(dict):
    """A dict that allows for object-like property access syntax."""

    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)


def demo():
    host = "10.118.136.234"
    port = 3001
    user = SYSTEM_ACCOUNT_AURORA
    password = ""
    database = "postgres"

    db = None
    try:
        db = Connection(host, port, user, password, database)
        for repl in db.query("select * from pg_stat_replication"):
            print(repl.sent_location, repl.client_addr)
    finally:
        if db is not None:
            db.close()


if __name__ == "__main__":
    demo()
