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

import datetime
import json
import os
import shutil
import signal
import subprocess
import sys
import time

from pg_tasks.install_instance import (
    build_recovery_conf,
    build_hba_conf,
    post_installation,
)
from pg_tasks.modify_postgresql_conf import (
    build_postgresql_conf,
    modify_postgresql_conf,
)
from pg_utils.envs import engine_env
from pg_utils.logger import logger
from pg_utils.utils import pid_exists
from pg_utils.os_operate import exec_command, safe_rmtree, mkdir_paths, chown_paths
from pg_utils.pg_connection import Connection
from pg_utils.pg_const import (
    PATH,
    DEFAULT_DB,
    PGDATA,
    STOP_LOG,
    INS_LOCK_FILE,
    POSTGRESQL_CONF_DEMO,
    LOG_AGENT_DATA_DIR,
    RESTORE_DOWNLOADS_DIR,
    RESTORE_JOB_STATUS,
    RESTORE_JOB_WORKER,
    RESTORE_JOB_LOG,
    SSL_CERT_PATH,
    SSL_KEY_PATH,
)
from pg_utils.pg_ctl import run_pgctl_cmd, check_postgres_is_running


def grow_pfs():
    pg_user = engine_env.get_initdb_user()
    if not check_postgres_is_running(pg_user, PATH, PGDATA):
        logger.warn("Instance is already stopped, skip grow_pfs")
        return

    pbd_info = engine_env.get_inst_attr(attr="pbd_list")[0]
    if engine_env.is_engine_type_on_pangu(pbd_info["engine_type"]):
        prefix = pbd_info["pbd_name"]
    elif engine_env.is_engine_type_on_san(pbd_info["engine_type"]):
        prefix, _ = engine_env.get_polar_storage_params()
    else:
        prefix = "%s-%s" % (pbd_info["pbd_number"], pbd_info["data_version"])

    polar_vfs_disk_expansion(prefix, pg_user, engine_env.get_server_port(), PGDATA)
    logger.info("grow_pfs successful")


def polar_vfs_disk_expansion(prefix, pg_user, port, pg_data, connect_password=""):
    if not prefix:
        raise Exception("Exception: prefix is empty")

    sql = "select polar_vfs_disk_expansion('%s')" % prefix
    logger.info("expand polar disk with sql: %s", sql)
    with Connection(pg_data, port, pg_user, connect_password, DEFAULT_DB) as conn:
        conn.execute(sql)


def create_stop_lock_file():
    try:
        if not os.path.exists(INS_LOCK_FILE):
            dirname = os.path.dirname(INS_LOCK_FILE)
            if not os.path.exists(dirname):
                os.makedirs(dirname)
            open(INS_LOCK_FILE, "a").close()
            logger.info("Successfully created stop lock file")
        else:
            logger.info("Stop Lock file %s already exists", INS_LOCK_FILE)
    except Exception as e:
        raise Exception(
            "Failed to create stop lock file %s, exception: %s"
            % (INS_LOCK_FILE, str(e))
        )


def remove_stop_lock_file():
    try:
        if os.path.exists(INS_LOCK_FILE):
            os.remove(INS_LOCK_FILE)
            logger.info("Successfully removed stop lock file %s", INS_LOCK_FILE)
        else:
            logger.info("Stop lock file %s not exist", INS_LOCK_FILE)
    except Exception as e:
        raise Exception(
            "Failed to remove stop lock file %s, exception: %s"
            % (INS_LOCK_FILE, str(e))
        )


def lock_stop_instance(lock=True):
    begin_time = datetime.datetime.utcnow()

    if lock:
        create_stop_lock_file()

    pg_user = engine_env.get_initdb_user()
    if not check_postgres_is_running(pg_user, PATH, PGDATA):
        logger.warn("Instance is already stopped, skip stop")
        return

    args = "-m %s" % engine_env.shutdown_mode
    try:
        run_pgctl_cmd(
            pg_user,
            PATH,
            PGDATA,
            "stop",
            STOP_LOG,
            args=args,
            time_out=engine_env.shutdown_timeout,
        )
    except Exception as e:
        logger.warn("shutdown failed: %s", str(e))
        if "server does not shut down" in str(e):
            logger.info("execute shutdown_cleanup.sh")
            exec_command("sh /shutdown_cleanup.sh", 60)

    # pg_ctl正常返回，但是pg进程可能还没有马上退出，不断检查直到退出或者超时
    while True:
        if not check_postgres_is_running(pg_user, PATH, PGDATA):
            break

        if (
            datetime.datetime.utcnow() - begin_time
        ).total_seconds() > engine_env.shutdown_timeout:
            logger.info("shutdown timeout, execute shutdown_cleanup.sh")
            exec_command("sh /shutdown_cleanup.sh", 60)
        time.sleep(1)

    # https://work.aone.alibaba-inc.com/issue/23787647?spm=a2o8d.corp_prod_issue_detail_v2.0.0.5c1a38ccHESPoo
    if engine_env.shutdown_cleanup:
        cmd = "ps --no-headers -eo pid,ppid,cmd"
        status, stdout = exec_command(cmd)
        if status != 0:
            raise Exception("execute cmd failed, %s, %s" % (cmd, stdout))
        else:
            candidates = {}
            cur_pid = str(os.getpid())
            for line in stdout.split("\n"):
                parts = line.strip().split(None, 2)
                if len(parts) != 3:
                    logger.warn("ignore process: %s", line)
                    continue
                pid, ppid, cmd = parts
                # exclude docker entrypoint, To prevent the possibility of causing container hang. https://work.aone.alibaba-inc.com/issue/28529668
                entrypoint = ["supervisor.py", "init_and_pause.py"]
                if (
                    pid in ("1", cur_pid)
                    or ppid != "0"
                    or any(s in cmd for s in entrypoint)
                ):
                    continue
                candidates[pid] = line

            for pid, info in candidates.items():
                try:
                    logger.info("killing process %s", info)
                    os.kill(int(pid), signal.SIGKILL)
                except Exception as e:
                    raise Exception("kill process failed, %s, %s" % (info, str(e)))

    logger.info("Successfully stopped postgres")


def unlock_start_instance():
    remove_stop_lock_file()

    pg_user = engine_env.get_initdb_user()
    if check_postgres_is_running(pg_user, PATH, PGDATA):
        logger.warn("Instance is already running, skip start")
        return

    # start by engine supervisor
    # logger.info("unlock_start_instance LD_LIBRARY_PATH is: %s", os.getenv("LD_LIBRARY_PATH"))
    # run_pgctl_cmd(pg_user, PATH, PGDATA, "start", START_LOG)


def restart_instance():
    lock_stop_instance()
    unlock_start_instance()


def build_pgsql_conf():
    pcmd = 'su - %s -c "cat %s > %s/postgresql.conf "' % (
        engine_env.get_initdb_user(),
        POSTGRESQL_CONF_DEMO,
        PGDATA,
    )
    logger.info("Run cmd: %s", pcmd)
    status, stdout = exec_command(pcmd)
    if status != 0:
        raise Exception("ERROR: Run cmd error: %s" % stdout)
    else:
        logger.info("build empty postgresql.conf successfully!")
    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    build_postgresql_conf(
        postgres_conf_file,
        engine_env.get_server_port(),
        engine_env.get_envs(),
        engine_env.db_version,
        engine_env.db_type,
        engine_env.is_tde_enable,
    )
    logger.info("Build postgresql.conf successfully!")

    params = dict()
    polar_disk_name, polar_datadir = engine_env.get_polar_storage_params()
    params["polar_hostid"] = int(engine_env.polarfs_host_id)
    params["polar_disk_name"] = "'%s'" % polar_disk_name
    params["polar_datadir"] = "'%s'" % polar_datadir
    modify_postgresql_conf(postgres_conf_file, params)
    logger.info("add postgresql.conf polarfs param successfully!")


def rebuild_local_dir():
    # clear PGDATA directory except INS_LOCK_FILE and run polar-replica-initdb.sh to rebuild local PGDATA
    for file in os.listdir(PGDATA):
        if os.path.basename(INS_LOCK_FILE) == file:
            continue

        full_name = os.path.join(PGDATA, file)
        if os.path.isfile(full_name):
            os.remove(full_name)
        elif os.path.isdir(full_name):
            shutil.rmtree(full_name)

    initdb_user = engine_env.get_initdb_user()

    _, polar_datadir = engine_env.get_polar_storage_params()
    initdb_cmd = 'su - %s -c "sh %s/polar-replica-initdb.sh %s/ %s/ "' % (
        initdb_user,
        PATH,
        polar_datadir,
        PGDATA,
    )
    logger.info("Run polar-replica-initdb cmd: %s", initdb_cmd)
    status, stdout = exec_command(initdb_cmd)
    if status != 0:
        raise Exception("Run initdb cmd error: %s" % stdout)
    else:
        logger.info("Run polar-replica-initdb cmd successfully!")

    """
    build recovery.conf for ro and standby
    build postgresql.conf for rw/ro/standby
    build pg_hba.conf for rw/ro/standby
    """
    build_pgsql_conf()
    build_hba_conf(initdb_user)
    if engine_env.is_polardb_pg_standby():
        build_recovery_conf({"standby_mode": "'on'"})
    elif engine_env.is_polardb_pg_ro():
        build_recovery_conf({"polar_replica": "'on'"})
    elif engine_env.is_polardb_pg_datamax():
        build_recovery_conf({"polar_datamax_mode": "'standalone'"})

    post_installation()


def setup_logagent_config():
    if not os.path.exists(LOG_AGENT_DATA_DIR):
        os.makedirs(LOG_AGENT_DATA_DIR)

    with open(os.path.join(LOG_AGENT_DATA_DIR, "insinfo"), "w") as fd:
        lines = [
            "[insinfo]",
            "base_collect_path=%s" % engine_env.base_collect_path,
            "pod_collect_path=%s" % engine_env.pod_collect_path,
            "ins_name=%s" % engine_env.ins_name,
            "port=%s" % engine_env.access_port,
            "container_mount_path=%s" % engine_env.container_mount_path,
            "host_mount_path=%s" % engine_env.host_mount_path,
        ]
        fd.write("\n".join(lines))

    with open(os.path.join(LOG_AGENT_DATA_DIR, "insname"), "w") as fd:
        fd.write("%s" % engine_env.ins_name)

    logger.info("Setup logagent config successfully")


def check_download_archive_status():
    # 检查pid，读取并返回结果
    with open(RESTORE_JOB_WORKER, "r") as fd:
        job_id = fd.read()
        exists = pid_exists(job_id)

    with open(RESTORE_JOB_STATUS, "r") as fd:
        job_status = json.loads(fd.read())
        total_tasks = job_status.get("total_tasks")
        completed_tasks = job_status.get("completed_tasks")
    total_count = len(total_tasks)
    completed_count = len(completed_tasks.keys())

    if exists:
        status = "running"
    else:
        status = "failed"

    if total_count == completed_count:
        status = "completed"

    result = {
        "status": status,
        "msg": "all wal files:%s, complete files:%s" % (total_count, completed_count),
    }
    logger.info(json.dumps(result))
    print(json.dumps(result))
    return result


def fetch_archive_log_from_source(docker_env):
    cmd = "mkdir -p %s" % RESTORE_DOWNLOADS_DIR
    exec_command(cmd)
    docker_env["srv_opr_action"] = "do_fetch_archive_log_from_source"
    f_log = open(RESTORE_JOB_LOG, "a")
    subprocess.Popen(
        sys.argv,
        env=docker_env,
        shell=True,
        stdin=None,
        stdout=f_log,
        stderr=f_log,
        close_fds=True,
    )


def do_fetch_archive_log_from_source():
    result = {}
    job_id = os.getpid()
    job_status = {}

    # Found another job, cancel it, and clear old download info
    try:
        with open(RESTORE_JOB_WORKER, "r") as fd:
            old_job_id = fd.read()
            os.kill(int(old_job_id), signal.SIGKILL)
            logger.info("fetch_archive_log: kill old fetch job %s" % old_job_id)
    except Exception as e:
        logger.info(e)

    with open(RESTORE_JOB_WORKER, "w+") as fd:
        fd.write(str(job_id))

    if os.path.exists(RESTORE_JOB_STATUS):
        with open(RESTORE_JOB_STATUS, "r") as fd:
            job_status = json.loads(fd.read())
    job_status["fetch_from_source_start_time"] = time.time()
    job_status["fetch_from_source_pid"] = job_id
    job_status["fetch_from_source_status"] = "running"

    with open(RESTORE_JOB_STATUS, "w") as fd:
        fd.write(json.dumps(job_status))
    """
    pitr_env = {
        "pitr_host": "",
        "pitr_port": "",
        "pitr_user": "",
        "pitr_password": ""
    }
    """
    pitr_env = engine_env.pitr_fetch_logs_env
    if pitr_env == {}:
        logger.exception("got no pitr env")
    sql = "select pg_current_wal_flush_lsn()"
    with Connection(
        pitr_env["pitr_host"],
        pitr_env["pitr_port"],
        pitr_env["pitr_user"],
        pitr_env["pitr_password"],
        DEFAULT_DB,
    ) as conn:
        try:
            # 获取当前主库位点
            data = conn.query(sql)
            flush_lsn = data[0]["pg_current_wal_flush_lsn"]
            end_lsn = (
                flush_lsn.split("/")[0]
                + "/"
                + hex(int(flush_lsn.split("/")[1], 16) - 1).split("x")[1].upper()
            )
        except Exception as e:
            result["status"] = "failed"
            result["msg"] = str(e)
            logger.info(json.dumps(result))
            return result
        # 获取到最新的wal日志
        cmd = "PGPASSWORD=%s pg_receivewal -p%s -h%s -U%s -D%s -E%s -v --no-loop" % (
            pitr_env["pitr_password"],
            pitr_env["pitr_port"],
            pitr_env["pitr_host"],
            pitr_env["pitr_user"],
            RESTORE_DOWNLOADS_DIR,
            end_lsn,
        )
        status, err = exec_command(cmd, timeout=320)
        if status != 0:
            result["status"] = "failed"
            result["msg"] = "pg_receivewal exec failed: %s" % err
        else:
            exec_command("rename .partial '' %s/*" % RESTORE_DOWNLOADS_DIR)
            result["status"] = "completed"
            result["msg"] = "pg_receivewal done, end lsn: %s" % end_lsn

            # Move all downloads to pfs and clear files
            move_log_to_pbd()

        job_status["fetch_from_source_status"] = result["status"]
        with open(RESTORE_JOB_STATUS, "w") as fd:
            fd.write(json.dumps(job_status))

    logger.info(json.dumps(result))
    print(json.dumps(result))
    return result


def check_fetch_archive_from_source_status():
    # 检查pid，读取并返回结果
    with open(RESTORE_JOB_WORKER, "r") as fd:
        job_id = fd.read()
        exists = pid_exists(job_id)

    with open(RESTORE_JOB_STATUS, "r") as fd:
        job_status = json.loads(fd.read())
        fetch_from_source_status = job_status.get("fetch_from_source_status", "running")
        fetch_from_source_pid = job_status.get("fetch_from_source_pid")

    status = "running"

    if not exists:
        status = fetch_from_source_status
        if fetch_from_source_status == "running":
            status = "failed"

    result = {
        "status": status,
        "msg": "status:%s, pid:%s, pid exists:%s"
        % (fetch_from_source_status, fetch_from_source_pid, exists),
    }
    logger.info(json.dumps(result))
    print(json.dumps(result))
    return result


def move_log_to_pbd():
    cluster_path = None
    pbd_info = engine_env.get_inst_attr(attr="pbd_list")[0]
    if not engine_env.is_engine_type_on_pangu(pbd_info["engine_type"]):
        pls_prefix, _ = engine_env.get_polar_storage_params()
    else:
        pls_prefix = pbd_info["pbd_name"]
        cluster_path = pbd_info["cluster_path"]

    logger.info("moving those files to pbd")
    logger.info(os.listdir(RESTORE_DOWNLOADS_DIR))
    for log_file in os.listdir(RESTORE_DOWNLOADS_DIR):
        logger.info("moving %s to pbd" % log_file)
        if not cluster_path:
            cmd = "/usr/local/bin/pfs -C disk cp -D disk -f %s/%s /%s/data/pg_wal/" % (
                RESTORE_DOWNLOADS_DIR,
                log_file,
                pls_prefix,
            )
        else:
            cmd = "/usr/local/bin/pfs cp -D %s %s/%s /%s/data/pg_wal/" % (
                cluster_path,
                RESTORE_DOWNLOADS_DIR,
                log_file,
                pls_prefix,
            )
        logger.info("move_log_to_pbd cmd is %s" % cmd)
        # lease of pbd mount is 5min, so timeout shoud be greater than that
        status, _ = exec_command(cmd, timeout=320)
        if status != 0:
            raise Exception(
                "pfs cp pg_wal file error, status is %d, cmd is %s" % (status, cmd)
            )


def restore_prepared():
    # Instance should be stopped here and before. And we are about to start it after this step.
    if check_postgres_is_running(engine_env.get_initdb_user(), PATH, PGDATA):
        logger.exception("Instance is already running, stop it first")

    logger.info("creating recovery.conf")
    if engine_env.pitr_time == "":
        logger.exception("pitr_time is empty")
    recovery_conf = "%s/recovery.conf" % PGDATA
    status, _ = exec_command("grep 'recovery_target_time=' %s -n" % recovery_conf)
    if status == 0:
        exec_command(
            "sed -i \"s/recovery_target_time=.*$/recovery_target_time='%s'/g\" %s"
            % (engine_env.pitr_time, recovery_conf)
        )
    else:
        exec_command(
            'su -l %s -c "echo \\"recovery_target_time=\'%s\'\\" >> %s"'
            % (engine_env.get_initdb_user(), engine_env.pitr_time, recovery_conf)
        )

    status, _ = exec_command("grep 'restore_command=' %s -n" % recovery_conf)
    if status == 0:
        exec_command(
            "sed -i \"s/restore_command=.*$/restore_command=''/g\" %s" % recovery_conf
        )
    else:
        exec_command(
            'su -l %s -c "echo \\"restore_command=\'\'\\" >> %s"'
            % (engine_env.get_initdb_user(), recovery_conf)
        )

    logger.info("creating backup_label")
    disk, _ = engine_env.get_polar_storage_params()
    pfs_backup_label = "/%s/data/polar_exclusive_backup_label" % disk
    local_backup_label = "/%s/backup_label" % PGDATA
    cmd = "pfs -C disk cp -S disk -f %s %s" % (pfs_backup_label, local_backup_label)
    status, _ = exec_command(cmd)
    if status != 0:
        raise Exception(
            "pfs cp backup label error, status is %d, cmd is %s" % (status, cmd)
        )

    exec_command("chown postgres:postgres %s" % local_backup_label)
    exec_command("cat %s" % local_backup_label)

    logger.info("removing old logindex")
    exec_command("pfs -C disk rm -r /%s/data/pg_logindex" % disk, timeout=320)

    result = {"status": "completed", "msg": "restore_prepared done"}
    logger.info(json.dumps(result))
    print(json.dumps(result))
    return result


def check_restore_running_status():
    check_sql = "select pg_is_in_recovery()"
    resume_sql = "select pg_wal_replay_resume()"
    switch_wal_sql = "select pg_switch_wal()"
    checkpoint_sql = "checkpoint"
    result = {}
    with Connection(
        PGDATA,
        engine_env.get_server_port(),
        engine_env.get_initdb_user(),
        "",
        DEFAULT_DB,
    ) as conn:
        status_mapping = {
            False: "completed",
            True: "running",
        }
        try:
            data = conn.query(check_sql)
            logger.info(data)
            status = data[0]["pg_is_in_recovery"]
            if not status:
                rename_partial_ready_files()
                logger.info("recovery done, do switch wal and checkpoint")
                logger.info(conn.query(switch_wal_sql))
                logger.info(conn.execute(checkpoint_sql))
                logger.info("switch wal and checkpoint done, do restart and waiting")
                restart_instance()
                wait_until_postgres_started()
            else:
                logger.info("recovery running, do resume replay")
                conn.query(resume_sql)
            result["status"] = status_mapping.get(status, "failed")
            result["msg"] = ""
        except Exception as e:
            result["status"] = "failed"
            result["msg"] = str(e)
    logger.info(json.dumps(result))
    print(json.dumps(result))
    return result


def rename_partial_ready_files():
    logger.info("try to rename wal.partial.ready to wal.partial.done")
    with Connection(
        PGDATA,
        engine_env.get_server_port(),
        engine_env.get_initdb_user(),
        "",
        DEFAULT_DB,
    ) as conn:
        disk, _ = engine_env.get_polar_storage_params()
        # file looks like 000000010000000200000003.partial.ready
        sql = (
            "SELECT * from pg_ls_dir('/%s/data/pg_wal/archive_status/') as file where length(file) = 38"
            % disk
        )
        logger.info("rename_partial_ready_files sql is %s" % sql)
        partial_ready_files = conn.query(sql)
        logger.info("rename ready for %s" % partial_ready_files)
        for partial_ready_file in partial_ready_files:
            partial_file = partial_ready_file["file"].strip(".ready")
            logger.info("rename wal ready file for %s" % partial_file)
            logger.info(
                conn.execute("set polar_rename_wal_ready_file = '%s'" % partial_file)
            )
        logger.info("rename done for %s" % partial_ready_files)


def wait_until_postgres_started():
    while not check_postgres_is_running(engine_env.get_initdb_user(), PATH, PGDATA):
        logger.info("Instance hasn't start yet, waiting for it")
        time.sleep(5)

    while True:
        try:
            time.sleep(5)
            with Connection(
                PGDATA,
                engine_env.get_server_port(),
                engine_env.get_initdb_user(),
                "",
                DEFAULT_DB,
            ) as conn:
                data = conn.query("select 1 as return")
                logger.info(data)
                if data[0]["return"] == 1:
                    logger.info("Instance started")
                    return
        except Exception:
            logger.info("Instance starting")
            continue


def switch_new_wal():
    sql = "select pg_switch_wal()"
    logger.info("switch wal with sql: %s", sql)
    with Connection(
        PGDATA,
        engine_env.get_server_port(),
        engine_env.get_initdb_user(),
        "",
        DEFAULT_DB,
    ) as conn:
        conn.execute(sql)


def prepare_ssl_files(crt_content, key_content):
    # create cert and key file
    rc, _ = exec_command(
        'su -l %s -c "echo \\"%s\\" > %s"'
        % (engine_env.get_initdb_user(), crt_content, SSL_CERT_PATH)
    )

    if rc != 0:
        logger.exception("can't find create ssl cert file, ret code %d" % (rc))

    rc, _ = exec_command(
        'su -l %s -c "echo \\"%s\\" > %s"'
        % (engine_env.get_initdb_user(), key_content, SSL_KEY_PATH)
    )

    if rc != 0:
        logger.exception("can't find create ssl key file, ret code %d" % (rc))

    # change file permission to 600
    rc, _ = exec_command(
        'su -l %s -c "chmod 600 %s %s"'
        % (engine_env.get_initdb_user(), SSL_CERT_PATH, SSL_KEY_PATH)
    )

    if rc != 0:
        logger.exception("change ssl cert/key file permission fail, ret code %d" % (rc))

    logger.info("prepare ssl cert/key file done.")


def create_tablespace():
    create_tablespace_env = engine_env.create_tablespace_env
    if create_tablespace_env == {}:
        logger.exception("got no create_tablespace_env env")

    table_name = create_tablespace_env["tablespace_name"]
    table_path = create_tablespace_env["tablespace_path"]

    with Connection(
        PGDATA,
        engine_env.get_server_port(),
        engine_env.get_initdb_user(),
        "",
        DEFAULT_DB,
    ) as conn:
        sql = "select spcname from pg_tablespace where spcname='%s'" % table_name
        logger.info("select_tablespace with sql: %s", sql)
        data = conn.query(sql)
        logger.info("pg_tablespace: %s", data)
        if len(data) > 0:
            logger.info("tablespace %s already exists, skip", table_name)
            return

        if os.path.exists(table_path):
            logger.info("remove existing table path %s", table_path)
            safe_rmtree(table_path)

        mkdir_paths([table_path])
        chown_paths([table_path], engine_env.get_initdb_user())
        logger.info("successfully create tablespace path %s", table_path)

        sql = "create tablespace %s location '%s'" % (table_name, table_path)
        logger.info("create_tablespace with sql: %s", sql)
        conn.execute(sql)


def add_dma_follower_to_cluster():
    with Connection(
        PGDATA,
        engine_env.get_server_port(),
        engine_env.get_initdb_user(),
        "",
        DEFAULT_DB,
    ) as conn:
        # TODO 修改为真实要加入的节点ip和port
        sql = "alter system dma add follower '%s:%s';" % (
            engine_env.get_inst_attr(),
            engine_env.port,
        )
        logger.info("Add dma follower with sql: %s", sql)
        data = conn.query(sql)
        logger.info("Add dma follower response: %s", data)


# 给cm调用
def update_recovery_conf():
    conf_path = os.path.join(PGDATA, "recovery.conf")
    with open(conf_path, "w") as fd:
        for k, v in engine_env.recovery_conf():
            fd.write("%s=%s\n", k, v)
    logger.info("successfully update recovery conf %s", conf_path)


def get_system_identifier():
    if engine_env.is_polardb_pg_ro():
        raise Exception("get system identifier on ro is not allowed")

    disk, _ = engine_env.get_polar_storage_params()
    pfs_control_file = "/%s/data/global/pg_control" % disk
    local_control_file = "/%s/global/pg_control" % PGDATA
    cmd = "pfs -C disk cp -S disk -f %s %s" % (pfs_control_file, local_control_file)
    status, stdout = exec_command(cmd, timeout=320)
    if status != 0:
        raise Exception("Run cmd error: %s" % stdout)
    else:
        logger.info(
            "successfully copy pg_control from pfs to local %s", local_control_file
        )

    cmd = "%s/pg_controldata -D %s" % (PATH, PGDATA)
    status, stdout = exec_command(cmd)
    if status != 0:
        raise Exception("Failed to get system identifier: %s" % stdout)

    # 通过stdout给operator传递结果
    print(stdout)
