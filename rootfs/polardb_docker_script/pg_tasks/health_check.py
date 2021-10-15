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
This file support health check

in addition to the common parameters of entry_point.py, the docker environment has additional parameters as like:

srv_opr_action:
             ---custins_check

"""
from pg_tasks.modify_postgresql_conf import get_instance_user
from pg_utils.logger import logger
from pg_utils.envs import engine_env
from pg_utils.pg_connection import Connection
from pg_utils.pg_const import DEFAULT_DB, PGDATA, DEFAULT_PORT


class HealthChecker:
    def __init__(self, docker_env):
        self.srv_opr_action = docker_env.get("srv_opr_action")
        self.user = get_instance_user(docker_env)
        self.port = int(engine_env.get_server_port())

    def do_action(self):
        if self.srv_opr_action == "hostins_check":
            custins_check(self.user, port=self.port)
        elif self.srv_opr_action == "service_check":
            custins_check(self.user, port=self.port)
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, "health_check")
            )


def custins_check(
    connect_user,
    connect_password="",
    connect_database=DEFAULT_DB,
    host=PGDATA,
    port=DEFAULT_PORT,
):
    sql_str = " select 1;"

    try:
        with Connection(
            host, port, connect_user, connect_password, connect_database
        ) as conn:
            conn.query(sql_str)
    except Exception as e:
        if "STANDBY_SNAPSHOT_PENDING" in str(e):
            logger.info(
                "The instance is under STANDBY_SNAPSHOT_PENDING state, regard as healthy"
            )
        raise e

    logger.info("The instance is healthy!")
