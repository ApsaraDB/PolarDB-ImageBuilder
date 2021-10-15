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

The instance stop function is here
in addition to the common parameters of entry_point.py, the docker environment has additional parameters as like:

srv_opr_physical_ins_id: the physical_ins_id
srv_opr_action:
             ---graceful_stop   "stop -m fast"

"""
import time

from pg_utils.logger import logger
from pg_utils.os_operate import add_os_user
from pg_utils.envs import engine_env
from pg_utils.parse_docker_env import get_instance_user
from pg_utils.pg_common import check_pid_pg_process, check_port_exists
from pg_utils.pg_const import PGDATA, PATH, STOP_LOG
from pg_utils.pg_ctl import run_pgctl_cmd


class StopInstance:
    def __init__(self, docker_env):
        self.srv_opr_timeout = docker_env.get("srv_opr_timeout")
        self.pg_user = get_instance_user(docker_env)
        self.port = engine_env.get_server_port()
        self.srv_opr_action = docker_env.get("srv_opr_action")
        self.container_name = docker_env.get("engine_container_name")

    def do_action(self):
        if self.srv_opr_action == "graceful_stop":
            stop_instance(
                self.container_name,
                self.pg_user,
                PATH,
                PGDATA,
                STOP_LOG,
                self.port,
                "-m fast",
                self.srv_opr_timeout,
            )
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, "stop")
            )


def stop_instance(
    container_name, pg_user, pg_bin_dir, pg_data, stop_log, port, args, time_out=300
):
    # The dbaas will check the contains health, so we do not need to check it run successfully
    if pg_user is None:
        raise Exception("We cannot get the pg_user, the docker env is wrong!")

    add_os_user(pg_user)
    logger.info(
        "We will stop the instance, the dbaas will check the contains exit successfully!"
    )
    run_pgctl_cmd(
        container_name,
        pg_user,
        pg_bin_dir,
        pg_data,
        "stop",
        stop_log,
        args,
        int(time_out),
    )


def check_instance_stopped(check_interval, timeout_check_shutdown, port, pid):
    logger.info("Start to check the instance stopped.")
    for _ in range(timeout_check_shutdown / check_interval):
        process_existed = check_pid_pg_process(pid)
        port_existed = check_port_exists(port)
        if process_existed or port_existed:
            time.sleep(check_interval)
        else:
            logger.info("PG instance has stopped.")
            return True
    else:
        logger.error("Cannot kill pg  process: %s after retried.", pid)
        return False
