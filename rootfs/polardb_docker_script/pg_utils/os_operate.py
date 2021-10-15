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
The os operate functions
"""
import grp
import os
import pwd
import shutil
import time

from pg_common import exec_command

from pg_utils.logger import logger
from pg_utils.pg_const import CORE_PATTERN_FILE, LOG_CORE_DIR


def mkdir_paths(path_list):
    for path in path_list:
        if not os.path.exists(path):
            os.makedirs(path)
            logger.info("Run: os.makedirs(%s)", path)


def chown_paths(path_list, user, mode="700"):
    for path in path_list:
        if os.path.exists(path):
            status, stdout = exec_command("chown -R %s %s" % (user, path))
            if status != 0:
                raise Exception("chown -R %s %s ERROR: %s" % (user, path, stdout))

            status, stdout = exec_command("chmod %s -R %s" % (mode, path))
            if status != 0:
                raise Exception("chmod %s -R  %s ERROR: %s" % (mode, path, stdout))
            logger.info("Run: os.chown(%s)", path)
        else:
            raise Exception("The path %s is not exist!" % path)


def rm_files_and_subdir(dir):
    """
    Remove the files and sub dir in dir, but contain the dir
    :param dir: the dst dir
    :return:
    """
    files_dirs_list = os.listdir(dir)
    try:
        for file_or_dir in files_dirs_list:
            file_or_dir_path = os.path.join(dir, file_or_dir)
            safe_rmtree(file_or_dir_path)
    except Exception as e:
        raise Exception(
            "we remove the files and subdirs in dir %s failed, out: %s" % (dir, str(e))
        )


def safe_rmtree(file_or_dir_path):
    """ remvoe dirname twice if failed """
    if not os.path.exists(file_or_dir_path):
        return
    if os.path.isdir(file_or_dir_path):
        try:
            shutil.rmtree(file_or_dir_path)
        except Exception as e:
            logger.error("Failed to remove dir %s, %s", file_or_dir_path, str(e))
            time.sleep(0.1)
            shutil.rmtree(file_or_dir_path)
    else:
        os.remove(file_or_dir_path)


def remove_file(file):
    if not os.path.exists(file):
        logger.info("file %s not exists, skip remove", file)
        return

    try:
        os.remove(file)
        logger.info("successfully remove file %s", file)
    except Exception as e:
        logger.error("failed to remove file %s, %s", file, str(e))


def is_os_user_exists(user, uid=None):
    logger.info("Check if user %s with uid %s exists", user, uid)

    try:
        user_pwd = pwd.getpwnam(user)
        if uid is not None:
            return user_pwd.pw_uid == uid
        return True
    except KeyError:
        return False


def add_os_user(user, uid=None):
    logger.info("Add system user %s", user)
    try:
        pwd.getpwnam(user)
        logger.info("The user %s exists, go on!", user)
    except KeyError:
        if not uid:
            status, stdout = exec_command("useradd %s" % user)
        else:
            status, stdout = exec_command("useradd -u %s %s" % (uid, user))
        if status != 0:
            raise Exception("We can not create the user %s with uid %d!" % (user, uid))


def is_os_group_exists(group):
    try:
        grp.getgrnam(group)
        return True
    except KeyError:
        return False


def is_user_in_group(user, group):
    groups = [g.gr_name for g in grp.getgrall() if user in g.gr_mem]
    if group not in groups:
        return False
    return True


def add_user_to_group(user, group):
    if group is None:
        return
    logger.info("Add user %s to group %s" % (user, group))
    if not is_os_group_exists(group):
        raise Exception("Group %s does not exist." % group)
    status, stdout = exec_command("usermod -a -G %s %s" % (group, user))
    if status != 0:
        raise Exception("We can not add user %s to group %s" % (user, group))


def remove_user_from_group(user, group):
    logger.info("Remove user %s from group %s ", user, group)
    if not is_os_group_exists(group):
        raise Exception("Group %s does not exist." % group)
    if not is_user_in_group(user, group):
        logger.info("User %s does not belong to group %s" % (user, group))
        return
    status, stdout = exec_command("gpasswd -d %s %s" % (user, group))
    if status != 0:
        logger.info("We can not remove user %s from %s group" % (user, group))
    # Double check, if user is in this group, raise exception.
    if is_user_in_group(user, group):
        raise Exception("Failed to remove user %s from %s group" % (user, group))


def del_os_user(user, remove_home=False):
    logger.info("Delete system user %s", user)
    try:
        pwd.getpwnam(user)
    except KeyError:
        logger.warn("User %s not exists, skip delete", user)
        return

    try:
        params = ""
        if remove_home:
            params = " %s -r " % params

        del_cmd = "userdel %s %s" % (params, user)
        exec_command(del_cmd)
    except Exception as e:
        raise Exception("We can not delete the user %s, %s!" % (user, str(e)))


def create_core_pattern_dir():
    src = os.path.abspath(LOG_CORE_DIR)
    mkdir_paths([src])

    with open(CORE_PATTERN_FILE, "r") as fd:
        core_pattern = fd.read().strip()
        # if start with |, It's mean root run next command
        if core_pattern.startswith("|"):
            core_pattern = core_pattern.replace("|", "")

        path = os.path.dirname(core_pattern)
        if path != "":
            dst = os.path.abspath(path)
            if not os.path.exists(dst):
                mkdir_paths([os.path.dirname(dst)])
                os.symlink(src, dst)
