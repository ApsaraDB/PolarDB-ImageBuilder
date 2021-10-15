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

import os

from pg_utils.logger import logger
from pg_utils.parse_docker_env import get_instance_user
from pg_utils.pg_connection import Connection
from pg_utils.pg_const import (
    DEFAULT_TDE_CLUSTER_COMMAND_PREFIX,
    PGDATA,
    DEFAULT_DB,
    SERVICE_TYPE_RW,
    SERVICE_TYPE_RO,
    SERVICE_TYPE_STANDBY,
    DEFAULT_TDE_SCRIPT,
)
from pg_utils.envs import engine_env
from pg_tasks.modify_postgresql_conf import modify_postgresql_conf
from pg_utils.os_operate import exec_command


class TDEManager:
    def __init__(self, docker_env):
        self.srv_opr_user_name = docker_env.get("srv_opr_user_name")
        self.srv_opr_user_password = docker_env.get("srv_opr_password")
        self.user = get_instance_user(docker_env)
        self.port = int(engine_env.get_server_port())
        self.password = ""

    def rotate_tde_key(self):
        tde_enable = engine_env.is_tde_enable
        if tde_enable:
            secret_get = engine_env.secret_get
            cluster_passphrase_command = "%s %s" % (
                DEFAULT_TDE_CLUSTER_COMMAND_PREFIX,
                secret_get,
            )
            update_kmgr_file(
                cluster_passphrase_command,
                self.user,
                self.port,
                engine_env.service_type,
            )
            update_cluster_passphrase_command()
        else:
            raise Exception("tde is not enable, but we want to rotate_tde_key?")


def update_kmgr_file(
    new_cluster_passphrase_command,
    pg_user,
    port,
    service_type=SERVICE_TYPE_RW,
    pg_data=PGDATA,
    database=DEFAULT_DB,
    connect_password="",
):
    sql = (
        "create extension if not exists polar_tde_utils;select polar_tde_update_kmgr_file('%s');"
        % new_cluster_passphrase_command
    )
    if service_type == SERVICE_TYPE_RO:
        return
    if service_type == SERVICE_TYPE_STANDBY:
        sql = (
            "select polar_tde_update_kmgr_file('%s');" % new_cluster_passphrase_command
        )
    logger.info("Execute update kek sql: %s", sql)

    with Connection(pg_data, port, pg_user, connect_password, database) as conn:
        conn.execute(sql)


def update_cluster_passphrase_command():
    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    params = dict()
    params["polar_cluster_passphrase_command"] = "'%s %s'" % (
        DEFAULT_TDE_CLUSTER_COMMAND_PREFIX,
        engine_env.secret_get,
    )
    modify_postgresql_conf(postgres_conf_file, params)


def copy_tde_script():
    copy_tde_script_cmd = (
        "cp /docker_script/pg_utils/tde_get_plain_dk.py %s;"
        "chmod +x %s" % (DEFAULT_TDE_SCRIPT, DEFAULT_TDE_SCRIPT)
    )
    logger.info("copy the tde_get_plain_dk script: %s", copy_tde_script_cmd)
    status, stdout = exec_command(copy_tde_script_cmd)
    if status != 0:
        raise Exception("Copy the tde_get_plain_dk script error: %s" % stdout)
    else:
        logger.info("Copy the tde_get_plain_dk script successfully!")
