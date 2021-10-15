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
import errno


MIN_UID = 10000


def check_not_null(kvs, keys=None):
    if keys is None:
        keys = kvs.keys()
    for k in keys:
        if not kvs[k]:
            raise Exception("Invalid key %s in dict %s" % (k, kvs))


# may conflict, but doesn't matter
def get_initdb_user_uid(logic_ins_id):
    return int(logic_ins_id) % MIN_UID + MIN_UID


# Check whether pid exists in the current process table.
def pid_exists(pid):
    pid = int(pid)
    if pid < 0:
        return False
    if pid == 0:
        raise ValueError("invalid PID 0")
    try:
        os.kill(pid, 0)
    except OSError as err:
        if err.errno == errno.ESRCH:
            return False
        elif err.errno == errno.EPERM:
            return True
        else:
            raise
    else:
        return True


def ip2int(ip):
    ipsegs = ip.split(".")
    ip_value = 0
    for seg in ipsegs:
        ip_value = (ip_value << 8) + int(seg)
    return ip_value


def int2ip(ip_value):
    ip = ""
    for i in range(4):
        ipseg = ip_value % 256
        ip = "%s.%s" % (ipseg, ip)
        ip_value /= 256
    ip = ip.strip(".")
    return ip
