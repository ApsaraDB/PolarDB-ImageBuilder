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
This is a install instance script. In this file, we just do:

1. mkdir PGDATA, LOG, CONF and chown them to pgxxx
2. export env to env file, source it before run cmd
3. exec initdb by user initdb_user, after run it successfully, we touch a initdb.done file
4. build postgresql.conf
5. build pg_hba.conf
6. start the postgresql

The docker env as follow:

ins_id: The instance id

physical_ins_id : The physical instance id

port: The list of port:
                      {\"205\": {\"access_port\": [3001], \"link\": [3001]}
                      The key of the dict is instance id 205
                      The port dict can get by the key portname. In the pg10, the portname is access_port and
                      the link indicate the port which will check custins
"""

import os
import shutil
import json

from pg_tasks.modify_pg_hba_conf import (
    add_replication_in_hba,
    add_superuser_in_hba,
    clear_hba_conf,
    add_user_in_hba,
    add_superuser_local_tcp_in_hba,
)
from pg_tasks.modify_postgresql_conf import build_postgresql_conf
from pg_tasks.modify_postgresql_conf import is_file_exist_use_pfs
from pg_tasks.modify_postgresql_conf import make_dir_use_pfs
from pg_tasks.modify_postgresql_conf import modify_postgresql_conf
from pg_tasks.modify_postgresql_conf import build_polar_dma_conf
from pg_utils.envs import engine_env
from pg_utils.logger import logger
from pg_utils.os_operate import (
    chown_paths,
    mkdir_paths,
    add_os_user,
    del_os_user,
    is_os_user_exists,
    add_user_to_group,
)
from pg_utils.pg_common import exec_command, is_share_storage
from pg_utils.pg_const import (
    PG_EXTERNAL_DATA,
    DMA_ROLE_MASTER,
    HBA_CONF_PATH,
    PRIVILEDGE_TYPE_REPLICATE,
    STORAGE_TYPE_POLAR_STORE,
    STORAGE_TYPE_FC_SAN,
    DEFAULT_TDE_CLUSTER_COMMAND_PREFIX,
    DEFAULT_TDE_FUNCTION_OPT,
)
from pg_utils.pg_const import (
    INS_CTX,
    PGDATA,
    LOG,
    POSTGRES_INITDB_ARGS,
    PATH,
    INS_INSTALL_STEP,
    POSTGRESQL_CONF_DEMO,
    INS_LOGIC_ID,
    ENGINE,
    SET_INSTALL_STEP_LOCK,
)
from pg_utils.properties import Properties
from pg_utils.utils import check_not_null, get_initdb_user_uid
from pg_tasks.update_tde_kek import copy_tde_script

import time

from pg_utils.os_operate import remove_file

INSTANCE_INSTALLATION_STEPS = ["prepare", "done"]


def build_hba_conf(initdb_user, enable_superuser_local_tcp=False):
    logger.info("Start to build pg_hba.conf")
    pg_hba_conf = os.path.join(PGDATA, "pg_hba.conf")
    clear_hba_conf(pg_hba_conf)
    if enable_superuser_local_tcp:
        add_superuser_local_tcp_in_hba(pg_hba_conf, initdb_user)
    add_superuser_in_hba(pg_hba_conf, initdb_user)
    add_replication_in_hba(pg_hba_conf, initdb_user)

    # HACK for polar box
    if os.path.exists("/etc/postgres/pg_hba.conf"):
        os.system("cat /etc/postgres/pg_hba.conf > %s" % pg_hba_conf)

    logger.info("Build pg_hba.conf successfully!")


def flush_recovery_conf(recovery_conf_path, params):
    props = Properties()
    if recovery_conf_path is not None:
        recovery_conf = open(recovery_conf_path)
        props.load(recovery_conf)
        recovery_conf.close()

    for key in params:
        props[str(key)] = params[str(key)]
    out = open(recovery_conf_path, "w")
    props.store(out)
    out.close()


def write_recovery_cnf(cust_params):
    primary_ins = engine_env.get_primary_ins_info()
    check_not_null(primary_ins, ["ins_ip", "port"])
    repl = engine_env.get_primary_account_by_privilege_type(PRIVILEDGE_TYPE_REPLICATE)[
        0
    ]
    logger.info(
        "Replica info %s %s %s %s",
        primary_ins["ins_ip"],
        str(primary_ins["port"]),
        repl["account"],
        repl["password"],
    )

    params = dict()
    slot_name = engine_env.get_slot_name(
        engine_env.service_type, engine_env.custins_id, engine_env.slot_unique_name
    )

    recovery_conf_file = os.path.join(PGDATA, "recovery.conf")
    params["recovery_target_timeline"] = "'latest'"
    params["primary_slot_name"] = "'%s'" % slot_name

    conn_info = "'host=%s port=%s user=%s password=%s application_name=%s '" % (
        primary_ins["ins_ip"],
        str(primary_ins["port"]),
        repl["account"],
        repl["password"],
        slot_name,
    )
    params["primary_conninfo"] = conn_info

    for k, v in cust_params.items():
        params[k] = v

    flush_recovery_conf(recovery_conf_file, params)
    logger.info("Add recovery.conf file successfully!")


def build_recovery_conf(custom_params):
    pcmd = 'su - %s -c "cat /recovery.conf.demo > %s/recovery.conf "' % (
        engine_env.get_initdb_user(),
        PGDATA,
    )
    logger.info("Run cmd: %s", pcmd)
    status, stdout = exec_command(pcmd)
    if status != 0:
        raise Exception("Run cmd error: %s" % stdout)
    else:
        logger.info("Build empty recovery.conf successfully!")
    write_recovery_cnf(custom_params)


def install_datamax_instance(initdb_user, polar_storage_cluster_name=""):
    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    polar_disk_name, polar_datadir = engine_env.get_polar_storage_params()

    pfs_inited = True
    if not is_file_exist_use_pfs(polar_datadir, polar_storage_cluster_name):
        pfs_inited = False
    logger.info("PFS inited: %s", pfs_inited)

    if pfs_inited:
        init_pfs(False, initdb_user, polar_datadir, polar_storage_cluster_name)

        pcmd = 'su - %s -c "cat %s > %s "' % (
            initdb_user,
            POSTGRESQL_CONF_DEMO,
            postgres_conf_file,
        )
        logger.info("Run cmd: %s", pcmd)
        status, stdout = exec_command(pcmd)
        if status != 0:
            raise Exception("Run cmd error: %s" % stdout)
        else:
            logger.info("Build empty postgresql.conf successfully!")
    else:
        # exec initdb by user initdb_user
        initdb_cmd = (
            'su - %s -c "%s/initdb -E UTF8 --locale=C -U %s %s -D %s -i %s"'
            % (
                initdb_user,
                engine_env.pg_base_bin_dir,
                initdb_user,
                POSTGRES_INITDB_ARGS,
                PGDATA,
                engine_env.primary_system_identifier,
            )
        )
        logger.info("Run initdb cmd: %s", initdb_cmd)
        status, stdout = exec_command(initdb_cmd)
        if status != 0:
            raise Exception("Run initdb cmd error: %s" % stdout)
        else:
            logger.info("Run initdb cmd successfully!")

    logger.info("Start to build postgresql.conf")
    build_postgresql_conf(
        postgres_conf_file,
        engine_env.get_access_port_from_port(),
        engine_env.get_envs(),
        engine_env.db_version,
        engine_env.db_type,
    )
    logger.info("Build postgresql.conf successfully!")

    polar_hostid = engine_env.polarfs_host_id
    if not polar_hostid:
        polar_hostid = 1
    params = {
        "polar_enable_shared_storage_mode": "on",
        "polar_hostid": polar_hostid,
        "polar_vfs.logic_ins_id": engine_env.logic_ins_id,
        "polar_disk_name": "'%s'" % polar_disk_name,
        "polar_datadir": "'%s'" % polar_datadir,
    }
    if polar_storage_cluster_name:
        params["polar_storage_cluster_name"] = polar_storage_cluster_name
    modify_postgresql_conf(postgres_conf_file, params)
    logger.info("Modify postgresql.conf successfully!")

    build_hba_conf(initdb_user, enable_superuser_local_tcp=True)
    add_user_in_hba(HBA_CONF_PATH, [["host", "all", "all", "0.0.0.0/0", "md5"]])
    logger.info("Build hba conf successfully!")

    # build polar-initdb.sh
    if not pfs_inited:
        init_pfs(True, initdb_user, polar_datadir, polar_storage_cluster_name)


def init_pfs(first_init, initdb_user, polar_datadir, polar_storage_cluster_name):
    make_dir_use_pfs(polar_datadir, polar_storage_cluster_name)

    if first_init:
        pinitdb_cmd = "sh %s/polar-initdb.sh %s/ %s/ %s" % (
            PATH,
            PGDATA,
            polar_datadir,
            polar_storage_cluster_name,
        )
    else:
        pinitdb_cmd = 'su - %s -c "sh %s/polar-replica-initdb.sh %s/ %s/ %s"' % (
            initdb_user,
            PATH,
            polar_datadir,
            PGDATA,
            polar_storage_cluster_name,
        )
    logger.info("Run cmd: %s", pinitdb_cmd)
    status, stdout = exec_command(pinitdb_cmd)
    if status != 0:
        raise Exception("Run cmd error: %s" % stdout)
    else:
        logger.info("Copy datadir to polar store successfully!")


def install_rw_instance(initdb_user, polar_storage_cluster_name=""):
    polar_disk_name, polar_datadir = engine_env.get_polar_storage_params()
    tde_enable = engine_env.is_tde_enable
    pfs_inited = True
    if not is_file_exist_use_pfs(polar_datadir, polar_storage_cluster_name):
        pfs_inited = False
    logger.info("PFS inited: %s", pfs_inited)

    if tde_enable:
        copy_tde_script()

    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    if pfs_inited:
        init_pfs(False, initdb_user, polar_datadir, polar_storage_cluster_name)

        pcmd = 'su - %s -c "cat %s > %s "' % (
            initdb_user,
            POSTGRESQL_CONF_DEMO,
            postgres_conf_file,
        )
        logger.info("Run cmd: %s", pcmd)
        status, stdout = exec_command(pcmd)
        if status != 0:
            raise Exception("Run cmd error: %s" % stdout)
        else:
            logger.info("Build empty postgresql.conf successfully!")
    else:
        # exec initdb by user initdb_user
        tde_opt = ""
        if tde_enable:
            secret_get = engine_env.secret_get
            cluster_passphrase_command = "%s %s" % (
                DEFAULT_TDE_CLUSTER_COMMAND_PREFIX,
                secret_get,
            )
            tde_opt = "--cluster-passphrase-command='%s' %s" % (
                cluster_passphrase_command,
                DEFAULT_TDE_FUNCTION_OPT,
            )

        initdb_cmd = 'su - %s -c "%s/initdb --username=%s %s -D %s %s"' % (
            initdb_user,
            engine_env.pg_base_bin_dir,
            initdb_user,
            POSTGRES_INITDB_ARGS,
            PGDATA,
            tde_opt,
        )
        logger.info("Run initdb cmd: %s", initdb_cmd)
        status, stdout = exec_command(initdb_cmd, 600)
        if status != 0:
            raise Exception("Run initdb cmd error: %s" % stdout)
        else:
            logger.info("Run initdb cmd successfully!")

    logger.info("Start to build postgresql.conf")
    build_postgresql_conf(
        postgres_conf_file,
        engine_env.get_access_port_from_port(),
        engine_env.get_envs(),
        engine_env.db_version,
        engine_env.db_type,
        tde_enable,
    )
    logger.info("Build postgresql.conf successfully!")

    params = dict()
    if polar_storage_cluster_name:
        params["polar_storage_cluster_name"] = polar_storage_cluster_name
    params["polar_vfs.logic_ins_id"] = engine_env.logic_ins_id
    params["polar_hostid"] = int(engine_env.polarfs_host_id)
    params["polar_disk_name"] = "'%s'" % polar_disk_name
    params["polar_datadir"] = "'%s'" % polar_datadir
    modify_postgresql_conf(postgres_conf_file, params)
    logger.info("Add postgresql.conf polarfs param successfully!")

    build_hba_conf(initdb_user)

    # build polar-initdb.sh
    if not pfs_inited:
        init_pfs(True, initdb_user, polar_datadir, polar_storage_cluster_name)


def install_ro_instance(initdb_user, standby_mode=False, polar_storage_cluster_name=""):
    # exec polar-replica-initdb by user initdb_user
    polar_disk_name, polar_datadir = engine_env.get_polar_storage_params()
    pinitdb_cmd = 'su - %s -c "sh %s/polar-replica-initdb.sh %s/ %s/ %s"' % (
        initdb_user,
        PATH,
        polar_datadir,
        PGDATA,
        polar_storage_cluster_name,
    )
    logger.info("Run polar-replica-initdb cmd: %s", pinitdb_cmd)
    status, stdout = exec_command(pinitdb_cmd)
    if status != 0:
        raise Exception("Run initdb cmd error: %s" % stdout)
    else:
        logger.info("Run polar-replica-initdb cmd successfully!")

    if engine_env.is_tde_enable:
        copy_tde_script()

    # build postgresql.conf
    pcmd = 'su - %s -c "cat %s > %s/postgresql.conf "' % (
        initdb_user,
        POSTGRESQL_CONF_DEMO,
        PGDATA,
    )
    logger.info("Run cmd: %s", pcmd)
    status, stdout = exec_command(pcmd)
    if status != 0:
        raise Exception("Run cmd error: %s" % stdout)
    else:
        logger.info("Build empty postgresql.conf successfully!")
    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    build_postgresql_conf(
        postgres_conf_file,
        engine_env.get_access_port_from_port(),
        engine_env.get_envs(),
        engine_env.db_version,
        engine_env.db_type,
        engine_env.is_tde_enable,
    )
    logger.info("Build postgresql.conf successfully!")

    params = dict()
    if polar_storage_cluster_name:
        params["polar_storage_cluster_name"] = polar_storage_cluster_name
    params["polar_vfs.logic_ins_id"] = engine_env.logic_ins_id
    params["polar_hostid"] = int(engine_env.polarfs_host_id)
    params["polar_disk_name"] = "'%s'" % polar_disk_name
    params["polar_datadir"] = "'%s'" % polar_datadir
    modify_postgresql_conf(postgres_conf_file, params)
    logger.info("Add postgresql.conf polarfs param successfully!")

    build_hba_conf(initdb_user)
    if standby_mode:
        build_recovery_conf({"standby_mode": "'on'"})
    else:
        build_recovery_conf({"polar_replica": "'on'"})


def install_normal_instance(initdb_user, standby_mode=False):
    # exec initdb by user initdb_user
    initdb_cmd = 'su - %s -c "%s/initdb --username=%s %s -D %s"' % (
        initdb_user,
        engine_env.pg_base_bin_dir,
        initdb_user,
        POSTGRES_INITDB_ARGS,
        PGDATA,
    )
    logger.info("Run initdb cmd: %s", initdb_cmd)
    status, stdout = exec_command(initdb_cmd)
    if status != 0:
        raise Exception("Run initdb cmd error: %s" % stdout)
    else:
        logger.info("Run initdb cmd successfully!")

    logger.info("Start to build postgresql.conf")
    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    build_postgresql_conf(
        postgres_conf_file,
        engine_env.get_access_port_from_port(),
        engine_env.get_envs(),
        engine_env.db_version,
        engine_env.db_type,
    )
    logger.info("Build postgresql.conf successfully!")

    build_hba_conf(initdb_user, enable_superuser_local_tcp=True)

    if standby_mode:
        build_recovery_conf({"standby_mode": "'on'"})


# https://yuque.antfin-inc.com/pg-hdb-dev/polardb/trpbwz
def install_dma_instance(initdb_user):
    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    dma_conf_file = os.path.join(PGDATA, "polar_dma.conf")
    if engine_env.dma_role == DMA_ROLE_MASTER:
        # exec initdb by user initdb_user
        initdb_cmd = 'su - %s -c "%s/initdb --username=%s %s -D %s"' % (
            initdb_user,
            engine_env.pg_base_bin_dir,
            initdb_user,
            POSTGRES_INITDB_ARGS,
            PGDATA,
        )
        logger.info("Run initdb cmd: %s", initdb_cmd)
        status, stdout = exec_command(initdb_cmd)
        if status != 0:
            raise Exception("Run initdb cmd error: %s" % stdout)
        else:
            logger.info("Run initdb cmd successfully!")

        polar_initdb_cmd = 'su - %s -c "%s/polar-initdb.sh %s/ %s/ localfs"' % (
            initdb_user,
            engine_env.pg_base_bin_dir,
            PGDATA,
            PG_EXTERNAL_DATA,
        )
        logger.info("Run polar initdb cmd: %s", polar_initdb_cmd)
        status, stdout = exec_command(polar_initdb_cmd)
        if status != 0:
            raise Exception("Run polar initdb error: %s" % stdout)
        else:
            logger.info("Run polar initdb cmd successfully")

        logger.info("Start to build postgresql.conf")
        build_postgresql_conf(
            postgres_conf_file,
            engine_env.get_access_port_from_port(),
            engine_env.get_envs(),
            engine_env.db_version,
            engine_env.db_type,
        )
        logger.info("Build postgresql.conf successfully!")

        polar_hostid = engine_env.polarfs_host_id
        if not polar_hostid:
            polar_hostid = 1
        modify_postgresql_conf(
            postgres_conf_file,
            {
                "polar_logindex_mem_size": 0,
                "polar_enable_xlog_buffer": "off",
                "polar_enable_shared_storage_mode": "on",
                "polar_enable_wal_prefetch": "false",
                "polar_vfs.localfs_test_mode": "on",
                "polar_hostid": polar_hostid,
                "polar_datadir": "'file-dio://%s'" % (PG_EXTERNAL_DATA),
            },
        )
        logger.info("Modify postgresql.conf successfully!")

        build_hba_conf(initdb_user, enable_superuser_local_tcp=True)
        add_user_in_hba(HBA_CONF_PATH, [["host", "all", "all", "0.0.0.0/0", "md5"]])
        logger.info("Build hba conf successfully!")
    else:
        primary_ins_info = engine_env.get_primary_ins_info()
        repl_account_info = engine_env.get_primary_account_by_privilege_type(
            PRIVILEDGE_TYPE_REPLICATE
        )[0]
        backup_cmd = 'su - %s -c "PGPASSWORD=%s %s/polar_basebackup -h %s -p %s -U %s -P -R -D %s --polardata=%s -X stream"' % (
            initdb_user,
            repl_account_info["password"],
            engine_env.pg_base_bin_dir,
            primary_ins_info["ins_ip"],
            primary_ins_info["port"],
            repl_account_info["account"],
            PGDATA,
            PG_EXTERNAL_DATA,
        )
        logger.info("Run polar_basebackup cmd: %s", backup_cmd)
        status, stdout = exec_command(backup_cmd)
        if status != 0:
            raise Exception("Run polar_basebackup error: %s" % stdout)
        else:
            logger.info("Run polar_basebackup cmd successfully")

    build_polar_dma_conf(dma_conf_file, engine_env)
    master_suffix = ""
    dma_join_key = "polar_dma_learners_info"
    if engine_env.dma_role == DMA_ROLE_MASTER:
        master_suffix = "@1"
        dma_join_key = "polar_dma_members_info"
    ins_info = engine_env.get_ins_info()
    cmd = 'su - %s -c "%s/polar-postgres -D %s -c polar_dma_init_meta=ON -c %s="%s:%s%s" -p %s"' % (
        initdb_user,
        engine_env.pg_base_bin_dir,
        PGDATA,
        dma_join_key,
        ins_info["ins_ip"],
        ins_info["port"],
        master_suffix,
        ins_info["port"],
    )
    logger.info("Run dma %s init cmd: %s", engine_env.dma_role, cmd)
    status, stdout = exec_command(cmd)
    if status != 0:
        raise Exception("Run dma %s init cmd error: %s" % (engine_env.dma_role, stdout))
    else:
        logger.info("Run dma %s init cmd successfully!", engine_env.dma_role)


def add_initdb_user(initdb_user, initdb_user_uid, group=None):
    if not is_os_user_exists(initdb_user, initdb_user_uid):
        logger.info(
            "Start to create initdb user %s with uid %s", initdb_user, initdb_user_uid
        )
        del_os_user(initdb_user, True)
        add_os_user(initdb_user, initdb_user_uid)
    else:
        logger.info("User %s with uid %s already exists", initdb_user, initdb_user_uid)
    add_user_to_group(initdb_user, group)
    return initdb_user, initdb_user_uid


def install_standby_instance(initdb_user, polar_storage_cluster_name):
    standby_rebuild_type = engine_env.get_standby_rebuild_type()
    if standby_rebuild_type == "rw":
        return install_rw_instance(initdb_user, polar_storage_cluster_name)
    elif standby_rebuild_type == "ro":
        return install_ro_instance(initdb_user, False, polar_storage_cluster_name)
    else:
        return backup_from_rw(initdb_user, polar_storage_cluster_name)
    pass


def install_instance():
    # add initdb user
    initdb_user = engine_env.get_initdb_user()
    initdb_user_uid = get_initdb_user_uid(engine_env.logic_ins_id)
    add_initdb_user(initdb_user, initdb_user_uid)

    # mkdir PGDATA, LOG and chown to user initdb_user
    need_mkdir = [PGDATA, LOG, PG_EXTERNAL_DATA]
    logger.info("Make dir %s and chown them to %s ", need_mkdir, initdb_user)
    mkdir_paths(need_mkdir)
    chown_paths(need_mkdir, initdb_user)

    # If the PGDATA is not empty, we have run initdb and modified conf file, skip it
    polar_storage_cluster_name = ""
    if engine_env.storage_type == STORAGE_TYPE_FC_SAN:
        polar_storage_cluster_name = "disk"
        chown_paths([engine_env.san_device_name], engine_env.get_initdb_user(), "660")

    if (
        os.path.exists(PGDATA)
        and not os.listdir(PGDATA)
        and not os.listdir(PG_EXTERNAL_DATA)
    ):
        if (
            engine_env.storage_type in [STORAGE_TYPE_POLAR_STORE, STORAGE_TYPE_FC_SAN]
            and engine_env.on_pfs
        ):
            # 共享存储
            if engine_env.is_polardb_pg_rw():
                install_rw_instance(initdb_user, polar_storage_cluster_name)
            elif engine_env.is_polardb_pg_ro():
                install_ro_instance(initdb_user, False, polar_storage_cluster_name)
            elif engine_env.is_polardb_pg_standby():
                # install_ro_instance(initdb_user, True, polar_storage_cluster_name)
                install_standby_instance(initdb_user, polar_storage_cluster_name)
                # backup_from_rw(initdb_user, polar_storage_cluster_name)
            elif engine_env.is_polardb_pg_datamax():
                install_datamax_instance(initdb_user, polar_storage_cluster_name)
            else:
                raise Exception("unknown engine type: %s" % engine_env.service_type)
        else:
            # 本地盘
            if not engine_env.dma_role:
                install_dma_instance(initdb_user)
            else:
                install_normal_instance(initdb_user, engine_env.is_polardb_pg_standby())
    else:
        logger.info(
            "The PGDATA or PG_EXTERNAL_DATA is exists and not empty, start postgres!"
        )

    pg_dst_log_dir = os.path.join(LOG, "pg_log")
    pg_src_log_dir = os.path.join(PGDATA, "log")
    if os.path.exists(pg_src_log_dir):
        if os.path.islink(pg_src_log_dir):
            pass
        elif os.path.isdir(pg_src_log_dir):
            clear_directory(pg_src_log_dir)
            os.rmdir(pg_src_log_dir)
        else:
            logger.info("Illegal log directory")
    if not os.path.exists(pg_dst_log_dir):
        os.makedirs(pg_dst_log_dir)
    if not os.path.exists(pg_src_log_dir):
        os.symlink(pg_dst_log_dir, pg_src_log_dir)
    chown_paths([pg_dst_log_dir, pg_src_log_dir], engine_env.get_initdb_user(), "775")


def backup_from_rw(initdb_user, polar_storage_cluster_name=""):
    primary_ins_info = engine_env.get_primary_ins_info()
    repl_account_info = engine_env.get_primary_account_by_privilege_type(
        PRIVILEDGE_TYPE_REPLICATE
    )[0]
    tde_enable = engine_env.is_tde_enable
    polar_disk_name, polar_datadir = engine_env.get_polar_storage_params()
    polar_host_id = engine_env.polarfs_host_id
    is_write_recovery_conf = engine_env.is_write_recovery_conf()
    if is_write_recovery_conf:
        write_recovery_flag = " -R"
    else:
        write_recovery_flag = " "

    backup_cmd = (
        'su - %s -c "PGPASSWORD=%s %s/polar_basebackup -h %s -p %s -U %s -P -D %s '
        "--polardata=%s  --polar_storage_cluster_name=%s "
        '--polar_disk_name=%s --polar_host_id=%s  %s -X stream"'
        % (
            initdb_user,
            repl_account_info["password"],
            engine_env.pg_base_bin_dir,
            primary_ins_info["ins_ip"],
            primary_ins_info["port"],
            repl_account_info["account"],
            PGDATA,
            polar_datadir,
            polar_storage_cluster_name,
            polar_disk_name,
            polar_host_id,
            write_recovery_flag,
        )
    )

    logger.info("Run polar_basebackup cmd: %s", backup_cmd)
    status, stdout = exec_command(backup_cmd)
    logger.info("Run polar_basebackup output [%s]" % stdout)
    if status != 0:
        raise Exception("Run polar_basebackup error: %s" % stdout)
    else:
        logger.info("Run polar_basebackup cmd successfully")

    remove_file(INS_INSTALL_STEP)
    # todo copy_tde_script??
    postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
    logger.info("Start to build postgresql.conf")
    build_postgresql_conf(
        postgres_conf_file,
        engine_env.get_access_port_from_port(),
        engine_env.get_envs(),
        engine_env.db_version,
        engine_env.db_type,
        tde_enable,
    )
    logger.info("Build postgresql.conf successfully!")

    params = dict()
    if polar_storage_cluster_name:
        params["polar_storage_cluster_name"] = polar_storage_cluster_name
    params["polar_vfs.logic_ins_id"] = engine_env.logic_ins_id
    params["polar_hostid"] = int(engine_env.polarfs_host_id)
    params["polar_disk_name"] = "'%s'" % polar_disk_name
    params["polar_datadir"] = "'%s'" % polar_datadir
    modify_postgresql_conf(postgres_conf_file, params)
    logger.info("Add postgresql.conf polarfs param successfully!")
    # #logic_ins_id修改 两个地方
    # logic_ins_id = os.getenv("logic_ins_id")


def is_installation_completed():
    if os.path.exists(INS_INSTALL_STEP):
        with open(INS_INSTALL_STEP, "r") as fd:
            if INSTANCE_INSTALLATION_STEPS[-1] == fd.read().strip():
                return True
    return False


def is_installation_prepare():
    if os.path.exists(SET_INSTALL_STEP_LOCK):
        with open(SET_INSTALL_STEP_LOCK, "r") as fd:
            if INSTANCE_INSTALLATION_STEPS[0] == fd.read().strip():
                return True
    return False


def wait_for_installation_completed():
    sleep_times = 0
    while True:
        if is_installation_completed():
            break

        if sleep_times % 60 == 0:
            logger.info("Instance is installing, wait and check later...")
        time.sleep(1)
        sleep_times += 1
    logger.info("Instance installation completed, ready to start")


def clear_directory(path):
    if not os.path.exists(path):
        return

    for root, dirs, files in os.walk(path):
        for f in files:
            os.remove(os.path.join(root, f))

        for d in dirs:
            shutil.rmtree(os.path.join(root, d))


def post_installation():
    from pg_tasks.host_operator import create_stop_lock_file

    if engine_env.lock_install_ins == "True":
        create_stop_lock_file()
    with open(INS_LOGIC_ID, "w") as fd:
        fd.write(engine_env.logic_ins_id)
        logger.info(
            "Successfully write logic_ins_id %s to %s",
            engine_env.logic_ins_id,
            INS_LOGIC_ID,
        )
    with open(INS_INSTALL_STEP, "w") as fd:
        fd.write(INSTANCE_INSTALLATION_STEPS[-1])
        logger.info("Successfully write installation step to %s", INS_INSTALL_STEP)
    with open(INS_CTX, "w") as fd:
        ctx = {
            "storage_type": engine_env.storage_type,
            "ins_logic_id": engine_env.logic_ins_id,
        }
        fd.write(json.dumps(ctx))


def wait_pfs_deamon_ready():
    # We wait for the pfsdeamon process no more than five times for 8 seconds each time.
    wait_pfs_deamon_sleep_count = 0
    while wait_pfs_deamon_sleep_count < 5:
        pfs_daemon = os.popen("pgrep pfsdaemon")
        pfs_daemon_pid = pfs_daemon.read()
        pfs_daemon.close()

        if pfs_daemon_pid:
            break
        time.sleep(8)
        wait_pfs_deamon_sleep_count += 1
        logger.info(
            "pfsdaemon process is not exist, Waiting for 8 seconds has been %s times.",
            wait_pfs_deamon_sleep_count,
        )
        if wait_pfs_deamon_sleep_count >= 5:
            break
    logger.info("The pfsd is ready, continue")


def setup_install_instance(source):
    if is_share_storage(engine_env.storage_type):
        wait_pfs_deamon_ready()

    if is_installation_completed():
        logger.info("The installation is already completed, skip")
        return

    if is_installation_prepare():
        logger.info("The installation is already prepare, skip")
        return

    logger.info("Installation is triggered by %s", source)
    # 兼容： source为'engine' 的都是已经在运行的老实例，slot等信息存储在本地，不能删除PGDATA目录
    if source != ENGINE:
        logger.info("Remove existing files in %s first", [PGDATA, PG_EXTERNAL_DATA])
        clear_directory(PGDATA)
        clear_directory(PG_EXTERNAL_DATA)

    # write prepare step for multi call
    with open(SET_INSTALL_STEP_LOCK, "w") as fd:
        fd.write(INSTANCE_INSTALLATION_STEPS[0])
        logger.info("Successfully write installation step to %s", SET_INSTALL_STEP_LOCK)

    try:
        install_instance()

        post_installation()

        create_ssl_files()

        create_tablespace_path()

        logger.info("The installation completed successfully")
    finally:
        logger.info(
            "Run end, remove set_install_step lock file %s", SET_INSTALL_STEP_LOCK
        )
        os.remove(SET_INSTALL_STEP_LOCK)


def create_ssl_files():
    from pg_tasks.host_operator import prepare_ssl_files

    ssk_key = os.getenv("ssl_key", "")
    ssl_cert = os.getenv("ssl_cert", "")
    if ssk_key != "" and ssl_cert != "":
        prepare_ssl_files(ssl_cert, ssk_key)


def create_tablespace_path():
    # 临时表目录
    create_tablespace_env = engine_env.create_tablespace_env
    if create_tablespace_env == {}:
        logger.exception("got no create_tablespace_env env")

    table_path = create_tablespace_env["tablespace_path"]
    logger.info("tablespace_path is %s", table_path)

    mkdir_paths([table_path])
    logger.info("tablespace_path make successfully")

    # if path exist, maybe rebuild. so clear data in table_path
    clear_directory(table_path)
    logger.info("Run: clear_directory(%s)", table_path)

    chown_paths([table_path], engine_env.get_initdb_user(), "775")
