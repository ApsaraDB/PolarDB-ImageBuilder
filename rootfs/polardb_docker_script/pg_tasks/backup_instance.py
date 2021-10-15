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
This file define action to backup a instance
"""
import datetime
import json

from pg_utils.logger import logger
from pg_utils.envs import engine_env
from pg_utils.parse_docker_env import get_instance_user
from pg_utils.pg_connection import Connection
from pg_utils.pg_const import DB_TYPE_PGSQL, DEFAULT_DB, PGDATA


class BackupInstance:
    def __init__(self, docker_env):
        self.srv_opr_action = docker_env.get("srv_opr_action")
        self.user = get_instance_user(docker_env)
        self.port = int(engine_env.get_server_port())
        self.password = ""
        self.db_type = docker_env.get("db_type", DB_TYPE_PGSQL)

    def do_action(self):
        if self.srv_opr_action == "pre_action":
            self.start_backup()
        elif self.srv_opr_action == "post_action":
            self.stop_backup()
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, "backup")
            )

    def start_backup(self):
        # for idempotency, we must stop backup first, but not return the error if it failed
        self.run_stop_backup_sql_only()
        result = dict()
        now = datetime.datetime.now()
        pg_start_backup_sql = (
            "select * from pg_walfile_name_offset(pg_start_backup('%s', true));" % now
        )
        try:
            with Connection(
                PGDATA, self.port, self.user, self.password, DEFAULT_DB
            ) as conn:
                pg_start_backup_result = conn.query(pg_start_backup_sql)
                start_file_name = pg_start_backup_result[0]["file_name"]
                result["xlog_start_location"] = start_file_name
                logger.info("Start_backup result: %s", json.dumps(result))
        except Exception as e:
            raise Exception("Start_backup failed: %s" % str(e))

    def stop_backup(self):
        result = dict()
        pg_stop_backup_sql = "select * from pg_walfile_name_offset(pg_stop_backup());"
        try:
            with Connection(
                PGDATA, self.port, self.user, self.password, DEFAULT_DB
            ) as conn:
                pg_stop_backup_result = conn.query(pg_stop_backup_sql)
                stop_file_name = pg_stop_backup_result[0]["file_name"]
                result["xlog_stop_location"] = stop_file_name
                logger.info("Stop_backup result: %s", json.dumps(result))
        except Exception as e:
            raise ("stop_backup failed: %s" % str(e))

    def run_stop_backup_sql_only(self):
        pg_stop_backup_sql = "select * from pg_stop_backup();"
        try:
            with Connection(
                PGDATA, self.port, self.user, self.password, DEFAULT_DB
            ) as conn:
                conn.query(pg_stop_backup_sql)
        except Exception:
            pass
