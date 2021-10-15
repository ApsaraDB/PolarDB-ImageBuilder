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
This file define action to enable/disable ssl support in instance
"""

from pg_tasks.host_operator import prepare_ssl_files
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
    SSL_CERT,
    SSL_KEY,
)
from pg_utils.pg_ctl import run_pg_reload_conf


class SSLInstance:
    def __init__(self, docker_env):
        self.srv_opr_action = docker_env.get("srv_opr_action")
        self.srv_opr_type = docker_env.get("srv_opr_type")
        self.user = get_instance_user(docker_env)
        self.port = int(engine_env.get_server_port())
        self.password = ""
        self.ssl_key = docker_env.get("ssl_key")
        self.ssl_cert = docker_env.get("ssl_cert")

    def do_action(self):
        if self.srv_opr_action == "enable_ssl":
            self.enable_ssl()
        elif self.srv_opr_action == "disable_ssl":
            self.disable_ssl()
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, self.srv_opr_type)
            )

    def enable_ssl(self):
        prepare_ssl_files(self.ssl_cert, self.ssl_key)
        params = {
            "ssl": "on",
            "ssl_cert_file": "'%s'" % SSL_CERT,
            "ssl_key_file": "'%s'" % SSL_KEY,
        }
        modify_postgresql_conf(POSTGRES_CONF_PATH, params)
        run_pg_reload_conf(self.user, PATH, PGDATA)
        logger.info("Enable ssl support successfully!")

    def disable_ssl(self):
        params = {"ssl": "off"}
        modify_postgresql_conf(POSTGRES_CONF_PATH, params)
        run_pg_reload_conf(self.user, PATH, PGDATA)
        logger.info("Disable ssl support successfully!")


def db_ssl_ready(port, connect_user, connect_password="", host=PGDATA):
    with Connection(host, port, connect_user, connect_password, DEFAULT_DB) as conn:
        sql = "show ssl;"
        result = conn.execute(sql)
        param_value = result[0]["ssl"]
        if param_value == "off":
            return False

        sql = "show ssl_cert_file;"
        result = conn.execute(sql)
        param_value = result[0]["ssl_cert_file"]
        if param_value != SSL_CERT:
            return False

        sql = "show ssl_key_file;"
        result = conn.execute(sql)
        param_value = result[0]["ssl_key_file"]
        if param_value != SSL_KEY:
            return False

        return True
