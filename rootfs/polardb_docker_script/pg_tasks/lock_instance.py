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
This file define action to lock/unlock a instance
"""

from pg_tasks.host_operator import lock_stop_instance, unlock_start_instance
from pg_tasks.modify_postgresql_conf import modify_postgresql_conf
from pg_utils.logger import logger
from pg_utils.envs import engine_env
from pg_utils.parse_docker_env import get_instance_user
from pg_utils.pg_connection import Connection
from pg_utils.pg_const import (
    POSTGRES_CONF_PATH,
    PGDATA,
    DEFAULT_DB,
    PATH,
)
from pg_utils.pg_ctl import run_pg_reload_conf


class LockInstance:
    def __init__(self, docker_env):
        self.srv_opr_action = docker_env.get("srv_opr_action")
        self.srv_opr_type = docker_env.get("srv_opr_type")
        self.user = get_instance_user(docker_env)
        self.port = int(engine_env.get_server_port())

    def do_action(self):
        if self.srv_opr_action == "lock_ins_diskfull":
            self.lock_ins_diskfull()
        elif self.srv_opr_action == "unlock_ins_diskfull":
            self.unlock_ins_diskfull()
        elif self.srv_opr_action == "lock_ins_expire":
            self.lock_ins_expire()
        elif self.srv_opr_action == "unlock_ins_expire":
            self.unlock_ins_expire()
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, self.srv_opr_type)
            )

    def lock_ins_diskfull(self):
        params = {"polar_force_trans_ro_non_sup": "on"}
        modify_postgresql_conf(POSTGRES_CONF_PATH, params)
        run_pg_reload_conf(self.user, PATH, PGDATA)
        do_killall_old_connections(self.port, self.user)
        logger.info("Set instance read only successfully!")

    def unlock_ins_diskfull(self):
        params = {"polar_force_trans_ro_non_sup": "off"}
        modify_postgresql_conf(POSTGRES_CONF_PATH, params)
        run_pg_reload_conf(self.user, PATH, PGDATA)
        logger.info("Set instance read/write successfully!")

    @staticmethod
    def lock_ins_expire():
        lock_stop_instance()
        logger.info("Lock expire instance successfully!")

    @staticmethod
    def unlock_ins_expire():
        unlock_start_instance()
        logger.info("Unlock expire instance successfully!")


def do_killall_old_connections(port, connect_user, connect_password="", host=PGDATA):
    sql = (
        "select pg_terminate_backend(pid) from pg_stat_activity "
        "where usename not in ('replicator', 'aurora', '%s') and pid != pg_backend_pid();"
        % connect_user
    )

    try:
        with Connection(host, port, connect_user, connect_password, DEFAULT_DB) as conn:
            conn.execute(sql)
    except Exception as e:
        raise Exception("Failed to kill old connection, Exception: %s" % str(e))

    logger.info("Kill all old connections successfully!")


def db_is_read_only(port, connect_user, connect_password="", host=PGDATA):
    sql = "show polar_force_trans_ro_non_sup;"

    with Connection(host, port, connect_user, connect_password, DEFAULT_DB) as conn:
        result = conn.execute(sql)
        param_value = result[0]["polar_force_trans_ro_non_sup"]
        if param_value == "off":
            return False
        elif param_value == "on":
            return True
