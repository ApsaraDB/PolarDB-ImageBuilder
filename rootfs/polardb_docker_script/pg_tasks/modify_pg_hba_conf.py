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
build pg_hba.conf file
"""
from pg_utils.logger import logger
from pg_utils.pg_common import exec_command


def clear_hba_conf(pg_hba_conf):
    """
    Clear hba conf before building the pg_hba.conf
    :param pg_hba_conf: the path of pg_hba.conf
    :return:
    """
    pg_hba_conf_file = open(pg_hba_conf, "w")
    pg_hba_conf_file.truncate()
    pg_hba_conf_file.close()
    logger.info("Clean the hba conf!")


def add_superuser_local_tcp_in_hba(pg_hba_conf, pguser):
    add_user_in_hba(pg_hba_conf, [["host", "all", pguser, "127.0.0.1/32", "trust"]])


# TODO superuser will all all md5
def add_superuser_in_hba(pg_hba_conf, pguser):
    """
    Add superuser info in pg_hba.conf
    :param pg_hba_conf: the path of pg_hba.conf
    :param pguser: the superuser like "pgxxx"
    :return:
    """
    userinfo = []
    userinfo.append(["local", "all", "all", "         ", "trust"])
    userinfo.append(["host", "all", pguser, "0.0.0.0/0", "reject"])
    # userinfo.append(['host', 'all', 'all', "0.0.0.0/0", 'md5'])
    add_user_in_hba(pg_hba_conf, userinfo)


# TODO superuser will all all md5
def add_replication_in_hba(pg_hba_conf, pguser, slave_ip=None, replicator=None):
    """
    Add replication in pg_hba.conf
    :param pg_hba_conf: the path of pg_hba.conf
    :param pguser: the superuser like "pgxxx"
    :param slave_ip: the slave_ip
    :param replicator: the user with replication privelege
    :return:
    """
    userinfo = []
    userinfo.append(["local", "replication", pguser, "         ", "trust"])
    userinfo.append(["host", "replication", pguser, "all", "reject"])
    if replicator is not None:
        userinfo.append(["local", "replication", replicator, "         ", "reject"])
    if replicator is not None and slave_ip is not None:
        userinfo.append(["host", "all", replicator, "all", "reject"])
        if ":" in slave_ip:
            userinfo.append(
                ["host", "replication", replicator, "%s/128" % slave_ip, "md5"]
            )
        else:
            userinfo.append(
                ["host", "replication", replicator, "%s/32" % slave_ip, "md5"]
            )
        userinfo.append(["host", "replication", replicator, "all", "reject"])
    userinfo.append(["host", "replication", "all", "all", "md5"])
    add_user_in_hba(pg_hba_conf, userinfo)


def add_user_in_hba(pg_hba_conf, userinfo):
    file = open(pg_hba_conf, "a")
    for userline in userinfo:
        line = " ".join(userline)
        file.write("%s\n" % line)
    file.close()
    logger.info("Add user in hba, userinfo: %s", userinfo)


def add_new_replicator_in_hba(pg_hba_conf, new_slave_ip):
    userinfo = ["host", "replication", "replicator", "%s/32" % new_slave_ip, "md5"]
    line = " ".join(userinfo)
    syscmd = (
        r"""grep -wq '^host replication replicator 0.0.0.0\/0 reject$' %s && echo 'Yes' || echo 'No' """
        % pg_hba_conf
    )
    logger.info("syscmd: %s", syscmd)
    stat, output = exec_command(syscmd)
    if stat == 0 and output.strip() == "No":
        logger.info("This is an old pg_hba.conf format instance.to compatible")
        add_user_in_hba(pg_hba_conf, userinfo)
        return
    elif stat == 0 and output.strip() == "Yes":
        syscmd = (
            r"""sed -i '/^host replication replicator 0.0.0.0\/0 reject$/i\%s' %s"""
            % (line, pg_hba_conf)
        )
        logger.info("syscmd: %s", syscmd)
        stat, output = exec_command(syscmd)
        if stat != 0:
            raise Exception("syscmd '%s' failed. output:%s" % (syscmd, output))
    else:
        raise Exception("syscmd '%s' failed. output:%s" % (syscmd, output))
