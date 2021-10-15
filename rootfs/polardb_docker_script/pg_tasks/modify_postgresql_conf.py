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
modify the postgresql.conf
"""
import commands
import json
import os

from pg_utils.envs import engine_env
from pg_utils.logger import logger
from pg_utils.parse_docker_env import get_instance_user
from pg_utils.pg_common import exec_command
from pg_utils.pg_const import (
    POSTGRESQL_CONF_DEMO,
    NEED_UPGRADE_POSTGRESQL_CONF_PARAMS,
    PGSQL_DB_VERSION,
    DB_TYPE_PGSQL,
    PGDATA,
    PATH,
    PRIVILEDGE_TYPE_REPLICATE,
    DEFAULT_TDE_CLUSTER_COMMAND_PREFIX,
)
from pg_utils.pg_ctl import run_pg_reload_conf
from pg_utils.properties import Properties
from pg_utils.os_operate import remove_file
from pg_utils.utils import ip2int


class PgConfiger:
    def __init__(self, docker_env):
        self.srv_opr_action = docker_env.get("srv_opr_action")

        if self.srv_opr_action == "create":
            raise Exception("WANING: skip make config file")
        elif self.srv_opr_action == "init_sysctl":
            self.mem_size = docker_env.get("mem_size")
            return
        self.user = get_instance_user(docker_env)
        self.docker_env = docker_env
        # TODO The db_version and db_type should be to pass by the docker env
        self.db_type = DB_TYPE_PGSQL
        self.db_version = PGSQL_DB_VERSION
        self.srv_opr_timeout = 300

    def do_action(self):
        postgres_conf_file = os.path.join(PGDATA, "postgresql.conf")
        postgres_auto_conf_file = os.path.join(PGDATA, "postgresql.auto.conf")
        if self.srv_opr_action == "update":
            param_values = json.loads(self.docker_env["params"])
            param_values = remove_private_keys(param_values)
            modify_postgresql_conf(postgres_conf_file, param_values)
            remove_file(postgres_auto_conf_file)
            if engine_env.reload_instance:
                run_pg_reload_conf(self.user, PATH, PGDATA)
        elif self.srv_opr_action == "init_sysctl":
            set_sysctl_conf(self.mem_size)
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, "config")
            )


def remove_private_keys(params):
    keys = [
        "polar_hostid",
        "polar_vfs.logic_ins_id",
        "polar_disk_name",
        "polar_datadir",
    ]
    for key in keys:
        value = params.pop(key, None)
        logger.info("Remove param: %s, value: %s", key, value)
    return params


def build_postgresql_conf(
    postgres_conf_file, port, docker_env, db_version, db_type, tde_enable=False
):
    """
    Modify postgresql.conf
    :param postgres_conf_file: the path of postgresql.conf
    :param docker_env: the env comes from the docker
    :param db_version: the database version
    :return:
    """
    params = dict()
    level_params = dict()
    params["port"] = port
    if db_version == PGSQL_DB_VERSION and db_type == DB_TYPE_PGSQL:
        if "mycnf_dict" in docker_env:
            level_params = json.loads(docker_env["mycnf_dict"])
        else:
            level_params = parse_postgresql_params_from_docker_env(
                docker_env, NEED_UPGRADE_POSTGRESQL_CONF_PARAMS
            )
    params.update(level_params)
    if tde_enable:
        params["polar_cluster_passphrase_command"] = "'%s %s'" % (
            DEFAULT_TDE_CLUSTER_COMMAND_PREFIX,
            engine_env.secret_get,
        )
    build_postgresql_conf_from_democfg(
        POSTGRESQL_CONF_DEMO, postgres_conf_file, **params
    )


def build_polar_dma_conf(dma_conf_file, engine_env):
    config = dict()
    ins_info = engine_env.get_ins_info()
    repl_account_info = engine_env.get_primary_account_by_privilege_type(
        PRIVILEDGE_TYPE_REPLICATE
    )[0]
    config.update(
        {
            "polar_enable_dma": "true",
            "polar_dma_repl_user": "'%s'" % repl_account_info["account"],
            "polar_dma_repl_passwd": "'%s'" % repl_account_info["password"],
            "polar_dma_repl_appname": "'standby_%s_%s'"
            % (ip2int(ins_info["ins_ip"]), ins_info["port"]),
        }
    )
    write_properties_cnf(config, None, dma_conf_file)


def build_postgresql_conf_from_democfg(democfg, outcfg, **params):
    """
    Build postgresql.conf from demo
    :param democfg: the path of demo postgresql.conf
    :param outcfg: the path of postgresql.conf
    :param params: a dict like {"max_connections":"2000",...}
    :return:
    """
    if not os.path.exists(democfg):
        raise Exception("the %s file is not exists" % democfg)

    if not os.path.exists(outcfg):
        raise Exception("the postgresql conf file is not exists")

    pgconfig = dict()
    pgconfig.update(params)
    write_properties_cnf(pgconfig, democfg, outcfg)


def modify_postgresql_conf(postgres_conf_path, params):
    props = Properties()
    if postgres_conf_path is not None:
        postgres_conf = open(postgres_conf_path)
        props.load(postgres_conf)
        postgres_conf.close()

    for key in params:
        if "max_conn" == key:
            continue
        props[str(key)] = params[str(key)]
    out = open(postgres_conf_path, "w")
    props.store(out)
    out.close()


def write_properties_cnf(params, democfg, outcfg):
    props = Properties()
    if democfg is not None:
        demo = open(democfg)
        props.load(demo)
        demo.close()

    for key in params:
        props[str(key)] = params[str(key)]
    out = open(outcfg, "w")
    props.store(out)
    out.close()


def parse_postgresql_params_from_docker_env(docker_env, param_name_list):
    params = dict()
    for param_name in param_name_list:
        if param_name in docker_env:
            params[param_name] = docker_env[param_name]
    return params


def set_sysctl_conf(mem_size):
    democfg = "/docker_script/sysctl.conf.demo"
    outcfg = "/etc/sysctl.conf"
    other_os_params = dict()
    if int(mem_size) >= 67108864:
        other_os_params = other_os_params.update(
            {"vm.nr_hugepages": 66536, "vm.lowmem_reserve_ratio": "1 1 1"}
        )
    if int(mem_size) >= 134217728:
        other_os_params = other_os_params.update(
            {"vm.extra_free_kbytes": 4096000, "vm.min_free_kbytes": 2097152}
        )
    # TODO kernel.shmall AND kernel.shmmax AND kernel.shmmin=819200 can not be set with the tools images. Refrence to Aone #14435455
    # other_os_params.update({'kernel.shmall':int(mem_size)*0.8*1024, 'kernel.shmmax':int(mem_size)*0.5*1024})
    write_properties_cnf(other_os_params, democfg, outcfg)
    sysctl_reload_cmd = "sysctl -p"
    status, stdout = exec_command(sysctl_reload_cmd)
    if status != 0:
        raise Exception("ERROR: %s!" % stdout)


def exec_linux_command(cmd, throw_exception=True):
    stat, output = commands.getstatusoutput(cmd)
    if stat != 0 and throw_exception:
        raise Exception("exec cmd %s failed. msg: %s" % (cmd, output))
    return stat, output


def is_file_exist_use_pfs(file_path, polar_storage_cluster_name=""):
    pfs_args = ""
    if polar_storage_cluster_name:
        pfs_args = "-C %s" % polar_storage_cluster_name
    cmd = "/usr/local/bin/pfs %s stat %s" % (pfs_args, file_path)
    stat, _ = exec_linux_command(cmd, False)
    if stat == 0:
        return True
    else:
        return False


def make_dir_use_pfs(dir_path, polar_storage_cluster_name=""):
    # query whether the dir already exist
    pfs_args = ""
    if polar_storage_cluster_name:
        pfs_args = "-C %s" % polar_storage_cluster_name
    cmd = "/usr/local/bin/pfs %s stat %s" % (pfs_args, dir_path)
    stat, _ = exec_linux_command(cmd, False)
    if stat == 0:
        logger.info("%s is already exist, now return", dir_path)
        return
    cmd = "/usr/local/bin/pfs %s mkdir %s" % (pfs_args, dir_path)
    exec_linux_command(cmd)
