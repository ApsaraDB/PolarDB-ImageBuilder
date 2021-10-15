#!/usr/bin/python
# _*_ coding:UTF-8
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

The user manager function is here
in addition to the common parameters of entry_point.py, the docker environment has additional parameters as like:

srv_opr_user_name: The user we managered
srv_opr_password: The password of the srv_opr_user_name
srv_opr_privilege: The privilege code as like:
                        NORMAL_ACCOUNT = 1------The account user created
                        PRIVILEDGE_TYPE_AURORA = 7-------The aurora
                        PRIVILEDGE_TYPE_REPLICATE = 15-------the replicator
                        PRIVILEDGE_TYPE_SUPER = 18--------the super user pgxxx
srv_opr_physical_ins_id: the physical_ins_id
srv_opr_action:
             ---create
             ---modify
             ---delete

"""
import psycopg2
import psycopg2.extras

from pg_utils.logger import logger
from pg_utils.envs import engine_env
from pg_utils.parse_docker_env import get_instance_user
from pg_utils.pg_connection import Connection
from pg_utils.pg_const import (
    DB_TYPE_PGSQL,
    NORMAL_ACCOUNT,
    PRIVILEDGE_TYPE_AURORA,
    PRIVILEDGE_TYPE_REPLICATE,
    PGDATA,
    DEFAULT_PORT,
    DEFAULT_DB,
    POLARDBADMIN_DB,
    ALIYUN_SUPER_ACCOUNT,
    RDS_INTERNAL_MARK,
    POLAR_INTERNAL_EXTENSIONS,
)


class UserManager:
    def __init__(self, docker_env):
        self.srv_opr_user_name = docker_env.get("srv_opr_user_name")
        self.srv_opr_user_password = docker_env.get("srv_opr_password")
        self.srv_opr_privilege = docker_env.get("srv_opr_privilege")
        self.srv_opr_action = docker_env.get("srv_opr_action")
        self.srv_opr_physical_ins_id = docker_env.get("srv_opr_physical_ins_id")
        self.user = get_instance_user(docker_env)
        self.port = int(engine_env.get_server_port())
        self.password = ""
        self.db_type = DB_TYPE_PGSQL

    def do_action(self):
        if self.srv_opr_action == "create":
            create_account(
                self.srv_opr_privilege,
                self.srv_opr_user_name,
                self.srv_opr_user_password,
                self.user,
                self.password,
                self.port,
                self.db_type,
            )
        elif self.srv_opr_action == "modify":
            modify_account(
                self.srv_opr_user_name,
                self.srv_opr_privilege,
                self.srv_opr_user_password,
                self.user,
                self.password,
                self.port,
                self.db_type,
            )
        elif self.srv_opr_action == "delete":
            delete_account(self.srv_opr_user_name, self.user, self.password, self.port)
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, "account")
            )


def is_new_user_exists(
    new_user,
    connect_user,
    connect_password="",
    connect_database=DEFAULT_DB,
    host=PGDATA,
    port=DEFAULT_PORT,
):
    sql_str = "select count(1) from pg_roles where rolname = '%s'" % new_user
    logger.info("Check the user exists sql: %s", sql_str)
    with Connection(
        host, port, connect_user, connect_password, connect_database
    ) as conn:
        rows = conn.query(sql_str)
        number = rows[0]["count"]
        if number == 1:
            return True
        else:
            return False


# TODO support mpd dbtype
def parse_privilege_code_to_priv_str(privilege_code, db_type=DB_TYPE_PGSQL):
    priv_str = None
    logger.info("The privilege_code is %s", privilege_code)

    superuser = False
    if privilege_code == NORMAL_ACCOUNT:
        priv_str = "nosuperuser createrole createdb login"
        superuser = False
    elif privilege_code == PRIVILEDGE_TYPE_AURORA:
        priv_str = "superuser login"
        superuser = True
    elif privilege_code == PRIVILEDGE_TYPE_REPLICATE:
        priv_str = "superuser replication login"
        superuser = True
    elif privilege_code == NORMAL_ACCOUNT:
        priv_str = "login"
        superuser = False
    elif privilege_code == ALIYUN_SUPER_ACCOUNT:
        priv_str = "polar_superuser replication login"
        superuser = False

    return priv_str, superuser


def create_account(
    privilege_code,
    new_user,
    new_user_password,
    connect_user,
    connect_password="",
    port=DEFAULT_PORT,
    db_type=DB_TYPE_PGSQL,
):
    priv_str, superuser = parse_privilege_code_to_priv_str(int(privilege_code), db_type)
    if priv_str is None:
        raise Exception("Do not support the privilege code %s" % privilege_code)

    if is_new_user_exists(new_user, connect_user, connect_password, port=port):
        sql = "alter role \"%s\" with %s password '%s';" % (
            new_user,
            priv_str,
            new_user_password,
        )
        logger.warn(
            'Warning: The account %s exists, alter role "%s" with %s password ******',
            new_user,
            new_user,
            priv_str,
        )
    else:
        sql = "create role \"%s\" with %s password '%s';" % (
            new_user,
            priv_str,
            new_user_password,
        )
        logger.info("Create role %s with %s password ******", new_user, priv_str)

    sqls = [sql]
    # 为避免超级用户受普通用户修改变量的影响，在创建超级用户时，运行下面的sql
    # https://work.aone.alibaba-inc.com/issue/22683851
    if superuser:
        sqls.extend(
            [
                "alter role %s set timezone='UTC'" % new_user,
                "alter role %s set datestyle='ISO,YMD'" % new_user,
                "alter role %s set extra_float_digits=0" % new_user,
                "alter role %s set lock_timeout=0" % new_user,
                "alter role %s set statement_timeout=0" % new_user,
                "alter role %s set temp_file_limit=10000000" % new_user,
                "alter role %s set idle_in_transaction_session_timeout=3600000"
                % new_user,
            ]
        )

    conn = None
    try:
        conn = psycopg2.connect(
            port=port,
            user=connect_user,
            host=PGDATA,
            database=DEFAULT_DB,
            options="-c DateStyle=ISO",
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        for sql in sqls:
            cursor.execute(RDS_INTERNAL_MARK + sql)
        conn.commit()
    finally:
        if conn:
            conn.close()

    # create internal extensions
    for extension in POLAR_INTERNAL_EXTENSIONS:
        # TODO 20200230: all the extension in postgres should be moved to polardb_admin, means that
        # we will only need to create extension in polardb_admin after 20200230.
        safe_create_extension(
            connect_user, port, PGDATA, DEFAULT_DB, extension, connect_password
        )
        safe_create_extension(
            connect_user, port, PGDATA, POLARDBADMIN_DB, extension, connect_password
        )

    logger.info("Create account %s successfully", new_user)


def delete_account(deleted_user, connect_user, connect_password="", port=DEFAULT_PORT):
    sql = 'DROP ROLE IF EXISTS "%s";' % deleted_user
    logger.info("Drop account with sql: %s", sql)
    with Connection(PGDATA, port, connect_user, connect_password, DEFAULT_DB) as conn:
        conn.execute(sql)

    logger.info("Drop account %s successfully", deleted_user)


def modify_account(
    modify_user,
    privilege_code,
    new_user_password,
    connect_user,
    connect_password="",
    port=DEFAULT_PORT,
    db_type=DB_TYPE_PGSQL,
):
    if not is_new_user_exists(modify_user, connect_user, connect_password, port=port):
        create_account(
            privilege_code,
            modify_user,
            new_user_password,
            connect_user,
            connect_password,
            port,
            db_type,
        )
        return
    priv_str, _ = parse_privilege_code_to_priv_str(int(privilege_code), db_type)
    sql_str = "alter role \"%s\" with %s password '%s';" % (
        modify_user,
        priv_str,
        new_user_password,
    )
    logger.info('Alter role "%s" with %s password ******', modify_user, priv_str)
    conn = None
    try:
        conn = psycopg2.connect(
            port=port,
            user=connect_user,
            host=PGDATA,
            database=DEFAULT_DB,
            options="-c DateStyle=ISO",
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(RDS_INTERNAL_MARK + sql_str)
        conn.commit()
    finally:
        if conn:
            conn.close()

    logger.info("Modify account %s successfully!", modify_user)


def is_slot_exists(
    slot_name,
    connect_user,
    connect_password="",
    connect_database=DEFAULT_DB,
    host=PGDATA,
    port=DEFAULT_PORT,
):
    sql_str = (
        "select count(1) from pg_replication_slots where slot_name = '%s' and pg_is_in_recovery()=false"
        % slot_name
    )
    logger.info("Check the slot exists sql: %s", sql_str)

    with Connection(
        host, port, connect_user, connect_password, connect_database
    ) as conn:
        rows = conn.query(sql_str)
        number = rows[0]["count"]
        if number == 1:
            return True
        else:
            return False


def create_slot(slot_name, connect_user, port=DEFAULT_PORT):
    sql = "select pg_create_physical_replication_slot('%s');" % slot_name
    logger.info("Create slot with sql %s", sql)
    conn = None
    try:
        conn = psycopg2.connect(
            port=port,
            user=connect_user,
            host=PGDATA,
            database=DEFAULT_DB,
            options="-c DateStyle=ISO",
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(RDS_INTERNAL_MARK + sql)
        conn.commit()
        logger.info("Create slot %s successfully", slot_name)
    except Exception as e:
        if "already exists" in str(e):
            logger.info("Replication slot %s already exists, skip", slot_name)
        else:
            raise e
    finally:
        if conn:
            conn.close()


def drop_slot(slot_name, connect_user, port=DEFAULT_PORT):
    sql = "select pg_drop_replication_slot('%s');" % slot_name
    logger.info("Drop slot with sql %s", sql)
    conn = None
    try:
        conn = psycopg2.connect(
            port=port,
            user=connect_user,
            host=PGDATA,
            database=DEFAULT_DB,
            options="-c DateStyle=ISO",
        )
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cursor.execute(RDS_INTERNAL_MARK + sql)
        conn.commit()
        logger.info("Drop slot %s successfully", slot_name)
    except Exception as e:
        if "does not exist" in str(e):
            logger.info("Replication slot %s does not exist, skip", slot_name)
        else:
            raise e
    finally:
        if conn:
            conn.close()


def safe_create_extension(
    pg_user, port, pg_data, database, extension, connect_password=""
):
    sql = "create extension if not exists %s" % extension
    logger.info("Execute create extension sql: %s", sql)

    with Connection(pg_data, port, pg_user, connect_password, database) as conn:
        conn.execute(sql)
