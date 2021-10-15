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

The restore repair function is here
in addition to the common parameters of entry_point.py, the docker environment has additional parameters as like:


srv_opr_action:
             ---repair_account

To support restore to a new instance, we should repair the account:

- rename the old super user pgxxx to the new super user pgxxx. xxx is the physical instance id
- rebuild and reload the pg_hba.conf. old pg_hba.conf is to old super user

"""
import os

from pg_tasks.manager_user import modify_account, is_new_user_exists
from pg_tasks.modify_pg_hba_conf import (
    clear_hba_conf,
    add_superuser_in_hba,
    add_replication_in_hba,
)
from pg_utils.logger import logger
from pg_utils.os_operate import add_os_user
from pg_utils.envs import engine_env
from pg_utils.parse_docker_env import get_instance_user
from pg_utils.pg_common import read_param_from_safe_cnf, update_safe_cnf
from pg_utils.pg_connection import Connection
from pg_utils.pg_const import (
    DB_TYPE_PGSQL,
    PRIVILEDGE_TYPE_AURORA,
    PGDATA,
    DEFAULT_PORT,
    DEFAULT_DB,
    PG_SAFE_CONF,
    SYSTEM_ACCOUNT_AURORA,
    PATH,
)
from pg_utils.pg_ctl import run_pg_reload_conf


class RestoreInstance:
    def __init__(self, docker_env):
        self.old_pg_user = get_old_pg_user_from_safe_conf(PG_SAFE_CONF)
        self.srv_opr_action = docker_env.get("srv_opr_action")
        self.user = get_instance_user(docker_env)
        self.port = int(engine_env.get_server_port())
        self.password = ""
        self.db_type = DB_TYPE_PGSQL

    def do_action(self):
        if self.srv_opr_action == "repair_account":
            rename_super_user(
                self.old_pg_user, self.user, port=self.port, db_type=self.db_type
            )
            rebuild_pg_hba_conf(self.user)
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, "restore")
            )


def rename_super_user(
    old_super_user,
    new_super_user,
    new_user_password="",
    old_user_password="",
    connect_user=SYSTEM_ACCOUNT_AURORA,
    connect_password="",
    port=DEFAULT_PORT,
    db_type=DB_TYPE_PGSQL,
):
    if not is_new_user_exists(
        new_super_user, connect_user, connect_password, port=port
    ):
        sql = 'ALTER USER "%s" RENAME TO "%s";' % (old_super_user, new_super_user)
        logger.info("We will rename user: %s", sql)
        with Connection(
            PGDATA, port, connect_user, connect_password, DEFAULT_DB
        ) as conn:
            conn.execute(sql)

        logger.info(
            "rename account %s to %s successfully", old_super_user, new_super_user
        )

    if new_user_password != old_user_password:
        modify_account(
            new_super_user,
            PRIVILEDGE_TYPE_AURORA,
            new_user_password,
            connect_user,
            connect_password=connect_password,
            port=port,
            db_type=db_type,
        )

    logger.info("update user in the safe conf!")
    pg_safe = dict(user=new_super_user)
    params = dict(pg_safe=pg_safe)
    update_safe_cnf(params, PG_SAFE_CONF)


def rebuild_pg_hba_conf(super_user):
    logger.info("Start to rebuild pg_hba.conf")
    pg_hba_conf = os.path.join(PGDATA, "pg_hba.conf")
    clear_hba_conf(pg_hba_conf)
    add_superuser_in_hba(pg_hba_conf, super_user)
    add_replication_in_hba(pg_hba_conf, super_user)
    logger.info("Rebuild pg_hba.conf successfully, start to reload the pg_hba.conf!")
    add_os_user(super_user)
    run_pg_reload_conf(super_user, PATH, PGDATA)


def get_old_pg_user_from_safe_conf(pg_safe_conf):
    return read_param_from_safe_cnf(pg_safe_conf, "pg_safe", "user")
