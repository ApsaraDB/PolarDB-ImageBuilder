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
pg ctl run
"""

from pg_utils.logger import logger
from pg_utils.pg_common import exec_command
from pg_utils.pg_const import RELOAD_LOG


def check_postgres_is_running(pg_user, pg_bin_dir, pg_data, time_out=300):
    pg_ctl_cmd = "%s/pg_ctl status -D %s" % (pg_bin_dir, pg_data)
    cmd = 'su -l %s -c "%s"' % (pg_user, pg_ctl_cmd)
    logger.info("Run pg_ctl cmd: %s", cmd)
    status, stdout = exec_command(cmd, time_out)
    if status == 0:
        return True

    return False


def run_pgctl_cmd(pg_user, pg_bin_dir, pg_data, action, log, args="", time_out=300):
    pg_ctl_cmd = "%s/pg_ctl %s -D %s -c -s -l %s %s" % (
        pg_bin_dir,
        action,
        pg_data,
        log,
        args,
    )
    cmd = 'su -l %s -c "%s"' % (pg_user, pg_ctl_cmd)
    logger.info("Run pg_ctl cmd: %s", cmd)
    status, stdout = exec_command(cmd, time_out)
    if status != 0:
        raise Exception("Run pg_ctl cmd error: %s" % stdout)
    else:
        logger.info("Run pg_ctl cmd successfully!")


def run_pg_reload_conf(pg_user, pg_bin_dir, pg_data, time_out=300):
    return run_pgctl_cmd(
        pg_user, pg_bin_dir, pg_data, "reload", RELOAD_LOG, time_out=time_out
    )
