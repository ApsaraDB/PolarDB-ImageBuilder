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
import subprocess
import sys
import time
import json

from pg_tasks.install_instance import (
    add_initdb_user,
    setup_install_instance,
    wait_for_installation_completed,
    wait_pfs_deamon_ready,
)
from pg_utils.logger import logger
from pg_utils.properties import Properties
from pg_utils.os_operate import (
    mkdir_paths,
    remove_user_from_group,
    create_core_pattern_dir,
)
from pg_utils.utils import get_initdb_user_uid
from pg_utils.os_operate import chown_paths
from pg_utils.pg_common import exec_command, is_share_storage
from pg_utils.pg_const import (
    PGDATA,
    PATH,
    INS_CTX,
    INS_LOCK_FILE,
    ALL_LIBRARY_PATHS,
    INS_LOGIC_ID,
    INITDB_SUPERUSER,
    ENGINE,
    HUGETLB_SHM_GROUP,
    PG_LOCK_FILE,
    STORAGE_TYPE_POLAR_STORE,
)

# init global vars
LD_LIBRARY_PATH = ":".join(filter(None, ALL_LIBRARY_PATHS))
os.environ["LD_LIBRARY_PATH"] = LD_LIBRARY_PATH


def wait_for_postgres_exit(pg_user, pg_bin_dir, pg_data, time_out=300):
    while True:
        if not check_postgres_is_running(pg_user, pg_bin_dir, pg_data, time_out):
            break
        time.sleep(1)


def check_postgres_is_running(pg_user, pg_bin_dir, pg_data, time_out=300):
    pg_ctl_cmd = "%s/pg_ctl status -D %s" % (pg_bin_dir, pg_data)
    cmd = 'su -l %s -c "%s"' % (pg_user, pg_ctl_cmd)
    status, _ = exec_command(cmd, time_out)
    if status == 0:
        return True

    return False


def is_instance_locked():
    return os.path.exists(INS_LOCK_FILE)


def wait_for_instance_unlocked():
    sleep_times = 0
    while True:
        if not is_instance_locked():
            break
        if sleep_times % 60 == 0:
            logger.info(
                "Found stop lock file %s, wait and check later...", INS_LOCK_FILE
            )
        time.sleep(1)
        sleep_times += 1
    logger.info("No stop lock file %s, instance is ready to start", INS_LOCK_FILE)


def get_pg_conf(key):
    props = Properties()
    value = None
    with open(os.path.join(PGDATA, "postgresql.conf")) as pcf:
        props.load(pcf)
        value = props.getProperty(key)
    return value


# Read postmaster.pid to get the instance status and check if it is ready
def is_instance_ready():
    lock_file = PGDATA + "/" + PG_LOCK_FILE
    try:
        with open(lock_file, "r") as f:
            lines = f.readlines()
            # the status is at the last line (now is 8th line).
            if len(lines) < 8:
                return False
            last_line = lines[-1]
            logger.info("Instance status is %s" % last_line)
            if last_line.strip() == "ready":
                return True
        return False
    except IOError:
        logger.info("Instance lock file %s does not exists." % lock_file)
        return False


def wait_until_remove_user_from_group(process, user, group):
    logger.info("Try to remove user %s from group %s" % (user, group))
    while not check_postgres_is_running(user, PATH, PGDATA):
        logger.info("Instance hasn't start yet, waiting for it")
        time.sleep(5)
        if process.poll() is None:
            continue
        else:
            return
    while process.poll() is None:
        time.sleep(5)
        if is_instance_ready():
            remove_user_from_group(user, group)
            return
        else:
            logger.info("Instance is not ready, waiting ...")
            continue


def check_is_preload_polar_perf_tool(filepath):

    is_preload = False
    if os.path.exists(filepath) is False:
        return is_preload

    f = open(filepath, "r")
    lines = f.readlines()
    for line in lines:
        if line.strip().startswith("#"):
            continue
        if line.find("shared_preload_libraries") != -1:
            if line.find("polar_perf_tool") != -1:
                is_preload = True
            else:
                is_preload = False

    f.close()

    return is_preload


def check_jemalloc_enable():
    jemalloc_so_file = "/usr/lib64/libjemalloc.so.2"
    ld_preload_prefix = "LD_PRELOAD=%s" % (jemalloc_so_file)

    if os.path.exists(jemalloc_so_file) and (
        check_is_preload_polar_perf_tool("/data/postgresql.conf")
        or check_is_preload_polar_perf_tool("/data/postgresql.auto.conf")
    ):
        return True, ld_preload_prefix

    return False, ld_preload_prefix


def run_supervisor():
    initdb_user = INITDB_SUPERUSER
    os.environ["INITDB_USER"] = initdb_user
    check_pfs_daemon_count = 0

    # 默认假设是共享存储
    storage_type = STORAGE_TYPE_POLAR_STORE
    while True:
        # 检查实例是否安装完成
        wait_for_installation_completed()

        # 检查是否存在stop锁
        wait_for_instance_unlocked()

        if not os.path.exists(INS_LOGIC_ID):
            raise Exception("instance logic id file %s not exists" % (INS_LOGIC_ID))
        with open(INS_LOGIC_ID, "r") as fd:
            initdb_user_uid = get_initdb_user_uid(fd.read().strip())
            # For allocating shared memory from huge page, we should add initdb user to HUGETLB_SHM_GROUP group.
            add_initdb_user(initdb_user, initdb_user_uid, HUGETLB_SHM_GROUP)

        # 如果ins_ctx不存在，也不影响实例启动，兼容manager没有升级的老实例
        if os.path.exists(INS_CTX):
            with open(INS_CTX, "r") as fd:
                ctx = json.loads(fd.read())
                storage_type = ctx["storage_type"]
                logger.info("found %s, use storage type %s", INS_CTX, storage_type)
        else:
            logger.info(
                "can not find %s, use default storage type %s", INS_CTX, storage_type
            )

        if is_share_storage(storage_type):
            wait_pfs_deamon_ready()

        check_pfs_daemon_count += 1

        polar_disk_name = get_pg_conf("polar_disk_name")
        if polar_disk_name:
            block_device_name = "/dev/%s" % polar_disk_name.strip("'").strip(
                '"'
            ).replace("_", "/", 1)
            if os.path.exists(block_device_name):
                chown_paths([block_device_name], user="postgres", mode=660)

        start_cmd = " ".join(sys.argv[1:])
        is_enable_jemalloc, ld_preload_prefix = check_jemalloc_enable()
        if is_enable_jemalloc:
            start_cmd = start_cmd.replace(
                "/u01/polardb_", ld_preload_prefix + " /u01/polardb_"
            )

        logger.info("Start the PostgreSQL! start_cmd:%s", start_cmd)
        p = subprocess.Popen(start_cmd, shell=True)

        # Wait instance start successfully, we remove initdb user from root group.
        # We only need the user in root group when instance is starting.
        wait_until_remove_user_from_group(p, initdb_user, HUGETLB_SHM_GROUP)

        _, err = p.communicate()
        logger.info("Postmaster exit with code %d, stderr: %s", p.returncode, err)

        # 存在stop锁，说明管控执行了stop_instance
        if is_instance_locked():
            continue

        # postgres进程正在运行，可能是管控已经执行了start_instance，等待postgres进程退出
        elif check_postgres_is_running(initdb_user, PATH, PGDATA):
            logger.info("PostgresSQL is already running, wait for it to exit")
            wait_for_postgres_exit(initdb_user, PATH, PGDATA)

        # 进程异常退出
        elif p.returncode != 0:
            return p.returncode


if __name__ == "__main__":

    logger.info("Start supervisor with env: %s", os.environ)

    exit_code = 0
    try:
        # add core_pattern dir link
        create_core_pattern_dir()

        # 兼容: operator还是老的，不会通过manager调用setup_install_instance，需要supervisor自行初始化
        if "cluster_custins_info" in os.environ:
            setup_install_instance(source=ENGINE)

        # 确保polartrace的目录存在，并且权限为777
        polar_trace_dirs = ["/dev/shm/polartrace", "/var/run/polartrace"]
        mkdir_paths(polar_trace_dirs)
        mode = 0o777
        for path in polar_trace_dirs:
            os.chmod(path, mode)
            for root, dirs, files in os.walk(path):
                for f in files:
                    os.chmod(os.path.join(root, f), mode)
                for d in dirs:
                    os.chmod(os.path.join(root, d), mode)

        exit_code = run_supervisor()
        logger.info("Engine exit with code %d", exit_code)
    except Exception as e:
        exit_code = -1
        logger.exception(e)
    sys.exit(exit_code)
