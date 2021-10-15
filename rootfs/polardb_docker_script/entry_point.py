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
This is a docker entrypoint script.
We will parse the os.env to decide what to do.

---------------------------The Docker Env Structure-----------------------------------------------

srv_opr_type:               The type of the task, include stop, account, health_check

srv_opr_action:             The action of the task, the srv_opr_type and srv_opr_action mapper as:
                            srv_opr_type          srv_opr_action
                            stop         -------- graceful_stop
                            account      -------- (create, modify, delete)
                            health_check -------- (hostins_check, custins_check)

srv_opr_timeout:            The timeout args for task

cluster_custins_info:       The cluster and custins info, like this(the pgsql is the pengine):
                                {
                                    "pgsql":
                                    {
                                        "custins_id":
                                        {
                                            "hostins":
                                            {
                                                "hostinsid":
                                                {
                                                    "ins_hostname": "xxx",
                                                    "vip": "xxx",
                                                    "role": "master",
                                                    "vport": "xxx",
                                                    "ins_ip": "xxx"
                                                }
                                            },
                                            "vips": [{"vport": "xxx", "net_type": "x", "vip": "xxx"}]
                                        }
                                    }
                                }

srv_opr_hosts:              The hostinfo, like this:
                                [{"ip":"", "access_port":[xxx], "physical_ins_id":xxx}]

The same type of tasks will have the same docker env, otherwise, docker env may be different.
The env above is the common part of the different task, the different part we will find in each file.

---------------------------------------------------------------------------------------------------


"""

import os
import sys

from pg_tasks.backup_instance import BackupInstance
from pg_tasks.health_check import HealthChecker
from pg_tasks.host_operator import (
    grow_pfs,
    lock_stop_instance,
    unlock_start_instance,
    restart_instance,
    rebuild_local_dir,
    setup_logagent_config,
    switch_new_wal,
    check_download_archive_status,
    restore_prepared,
    check_restore_running_status,
    fetch_archive_log_from_source,
    check_fetch_archive_from_source_status,
    do_fetch_archive_log_from_source,
    create_tablespace,
    add_dma_follower_to_cluster,
    update_recovery_conf,
    get_system_identifier,
)
from pg_tasks.install_instance import setup_install_instance
from pg_tasks.lock_instance import LockInstance
from pg_tasks.manager_replica import ReplicaManager
from pg_tasks.manager_user import UserManager
from pg_tasks.modify_postgresql_conf import PgConfiger
from pg_tasks.restore_instance import RestoreInstance
from pg_tasks.stop_instance import StopInstance
from pg_tasks.ssl_instance import SSLInstance
from pg_tasks.update_tde_kek import TDEManager
from pg_utils.logger import logger
from pg_utils.pg_const import ALL_LIBRARY_PATHS, SECRET_ENV_KEYS, MANAGER

# init global vars
LD_LIBRARY_PATH = ":".join(filter(None, ALL_LIBRARY_PATHS))
os.environ["LD_LIBRARY_PATH"] = LD_LIBRARY_PATH


# 管控传递的env的key一般是小写
def print_operation_envs(srv_opr_type, srv_opr_action, all_envs):
    operation_envs = {}
    for k, v in all_envs.items():
        if k.isupper():
            continue
        if k in SECRET_ENV_KEYS:
            v = "********"
        operation_envs[k] = v
    envlist = []
    for k, v in operation_envs.items():
        envlist.append("%s=%s" % (k, v))
    logger.info(
        "Manage request srv_opr_type: %s, srv_opr_action: %s, envs: \n%s\n",
        srv_opr_type,
        srv_opr_action,
        "\n".join(envlist),
    )


def entry(docker_env):
    srv_opr_action = docker_env.get("srv_opr_action")
    srv_opr_type = docker_env.get("srv_opr_type")

    print_operation_envs(srv_opr_type, srv_opr_action, docker_env)

    if srv_opr_type == "init_ins":
        if srv_opr_action == "initdb":
            logger.info("WANNING: skip initdb step")
            sys.exit(0)
        else:
            raise Exception("Unsupported operate action")
    elif srv_opr_type == "replica":
        replica_manager = ReplicaManager(docker_env)
        replica_manager.do_action()
    elif srv_opr_type == "stop":
        stop_instance = StopInstance(docker_env)
        stop_instance.do_action()
    elif srv_opr_type == "account":
        user_manager = UserManager(docker_env)
        user_manager.do_action()
    elif srv_opr_type == "health_check":
        health_checker = HealthChecker(docker_env)
        health_checker.do_action()
    elif srv_opr_type == "backup":
        instance_backuper = BackupInstance(docker_env)
        instance_backuper.do_action()
    elif srv_opr_type == "restore":
        instance_restorer = RestoreInstance(docker_env)
        instance_restorer.do_action()
    elif srv_opr_type == "lock_ins":
        instance_locker = LockInstance(docker_env)
        instance_locker.do_action()
    elif srv_opr_type == "update_conf":
        instance_configer = PgConfiger(docker_env)
        instance_configer.do_action()
    # 生成UE需要的监控元信息
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "setup_logagent_config":
        setup_logagent_config()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "get_system_identifier":
        get_system_identifier()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "grow_pfs":
        grow_pfs()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "build_recovery":
        update_recovery_conf()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "stop_instance":
        lock_stop_instance()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "start_instance":
        unlock_start_instance()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "restart_instance":
        restart_instance()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "setup_install_instance":
        setup_install_instance(source=MANAGER)
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "generate_new_wal":
        switch_new_wal()
    elif (
        srv_opr_type == "hostins_ops"
        and srv_opr_action == "check_download_archive_status"
    ):
        check_download_archive_status()
    elif (
        srv_opr_type == "hostins_ops"
        and srv_opr_action == "fetch_archive_log_from_source"
    ):
        fetch_archive_log_from_source(docker_env)
    elif (
        srv_opr_type == "hostins_ops"
        and srv_opr_action == "do_fetch_archive_log_from_source"
    ):
        do_fetch_archive_log_from_source()
    elif (
        srv_opr_type == "hostins_ops"
        and srv_opr_action == "check_fetch_archive_from_source_status"
    ):
        check_fetch_archive_from_source_status()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "restore_prepared":
        restore_prepared()
    elif (
        srv_opr_type == "hostins_ops"
        and srv_opr_action == "check_restore_running_status"
    ):
        check_restore_running_status()
    elif (
        srv_opr_type == "hostins_ops"
        and srv_opr_action == "add_dma_follower_to_cluster"
    ):
        add_dma_follower_to_cluster()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "enable_ssl":
        instance_ssl = SSLInstance(docker_env)
        instance_ssl.do_action()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "disable_ssl":
        instance_ssl = SSLInstance(docker_env)
        instance_ssl.do_action()
    elif srv_opr_action == "lock_stop_instance":
        lock_stop_instance()
    elif srv_opr_action == "unlock_start_instance":
        unlock_start_instance()
    elif srv_opr_action == "rebuild_local_dir":
        rebuild_local_dir()
    elif srv_opr_action == "rotate_tde_key":
        tde_manger = TDEManager(docker_env)
        tde_manger.rotate_tde_key()
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "process_cleanup":
        lock_stop_instance(lock=False)
    elif srv_opr_type == "hostins_ops" and srv_opr_action == "create_tablespace":
        create_tablespace()
    else:
        raise Exception("Not support the operator of %s type" % srv_opr_type)


if __name__ == "__main__":
    docker_env = os.environ

    try:
        entry(docker_env)
    except Exception as e:
        logger.exception(e)
        raise e
