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
The common fuctions:

exec_command: execute command
get_pgsql_process_id: get process id from postmaster.pid
check_pid_pg_process: check the postgresql process existed
check_port_exists: check the port listened

"""
import errno
import os
import socket
import subprocess
import sys
import time

from ConfigParser import ConfigParser

from pg_utils.logger import logger
from pg_utils.pg_const import PGDATA, STORAGE_TYPE_POLAR_STORE, STORAGE_TYPE_FC_SAN


def exec_command(cmd, timeout=180):
    def get_interval_iter():
        """a interval iterator"""
        MIN_INTERVAL, MAX_INTERVAL = 0.1, 1
        while True:
            yield min(MIN_INTERVAL, MAX_INTERVAL)
            if MIN_INTERVAL < MAX_INTERVAL:
                MIN_INTERVAL *= 2

    logger.info("Run command[timeout=%d]: %s", timeout, cmd)
    interval_iter = get_interval_iter()
    start_time = time.time()
    deadline = start_time + timeout
    close_fds = False if os.name == "nt" else True
    pipe = subprocess.Popen(
        cmd,
        shell=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        close_fds=close_fds,
    )
    pipe_fd = pipe.stdout.fileno()
    # return the integer 'file descriptor' used by the underlying implementation
    if os.name == "nt":
        # get the file handler of windows platform to call win32 API
        import msvcrt

        pipe_fd = msvcrt.get_osfhandle(pipe_fd)
    else:
        # set the file to Nonblock file
        import fcntl

        fcntl.fcntl(pipe_fd, fcntl.F_SETFL, os.O_NONBLOCK)
    output = ""
    if timeout:
        while time.time() < deadline:
            if pipe.poll() is not None:
                output += pipe.stdout.read()
                return pipe.returncode, output
            if os.name == "nt":
                import ctypes.wintypes

                c_avail = ctypes.wintypes.DWORD()
                # get the number of data in pipe
                ctypes.windll.kernel32.PeekNamedPipe(
                    pipe_fd, None, 0, None, ctypes.byref(c_avail), None
                )
                # print (pipe_fd, c_avail)
                # read all data of pipe in each second to avoid pipe to overflow
                if c_avail.value:
                    output += pipe.stdout.read(c_avail.value)
                else:
                    interval = next(interval_iter)
                    time.sleep(interval)
            else:
                # select function wait until pipe_fd is ready for reading
                import select

                rlist, _, _ = select.select([pipe_fd], [], [], next(interval_iter))
                if rlist:
                    try:
                        output += pipe.stdout.read(1024)
                    except IOError as e:
                        if e[0] != errno.EAGAIN:
                            raise
                        sys.exc_clear()
        pipe.stdin.close()
        pipe.stdout.close()
        try:
            pipe.terminate()
        except OSError as e:
            if e[0] != 5:
                raise
            for _ in range(10):
                try:
                    os.kill(pipe.pid, 9)
                except OSError as e:
                    if e[0] in (3, 87):
                        break
                    else:
                        time.sleep(1)
            else:
                logger.info("the process cannot be killed: %s", cmd)
        return 0x7F, "time out"
    else:
        pipe.wait()
        return pipe.returncode, pipe.stdout.read()


def get_pgsql_process_id():
    """
    Read pgsql process id from file postmaster.pid
    """
    pid_file = r"%s/postmaster.pid" % PGDATA
    with open(pid_file, "r") as f:
        fst_line = f.readline()
        try:
            int(fst_line.strip())
        except Exception as e:
            print("Exception: %s" % str(e))
            sys.exit(-1)
        return fst_line.strip()


def check_pid_pg_process(pid):
    """
    check whether a specified pid is a pg or mpd process
    grep "#custins_name#" /proc/#pid#/cmdline -a|grep postgres -a|wc -l
    """
    check_cmd = """ grep "%s" /proc/%s/cmdline -a|grep postgres -a|wc -l """ % (
        "postgres",
        pid,
    )
    return check_pid_process(check_cmd)


def check_pid_process(check_cmd):
    result, output = exec_command(check_cmd)
    if result != 0:
        return False
    output = output.strip()
    try:
        tmp_value = int(output)
        if tmp_value == 1:  # the process is existed.
            return True
        elif tmp_value == 0:
            return False
        else:
            logger.error(
                "Bad result while check process, cmd: %s, output: %s", check_cmd, output
            )
            return False
    except ValueError:
        return False


def check_port_exists(port, host="127.0.0.1"):
    try:
        s = socket.socket()
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((host, port))
        s.close()
        logger.info("The port is not listening socket!")
        return False
    except socket.error:
        logger.info("ERROR: The port is listening socket!")
        return True


def read_param_from_safe_cnf(mycnf_file, section, key):
    if not os.path.exists(mycnf_file):
        print("The file %s is not existed!" % mycnf_file)
        sys.exit(-1)
    config = ConfigParser(allow_no_value=True)
    config.read(mycnf_file)

    if not config.has_section(section):
        return None

    return config.get(section, key)


def update_safe_cnf(params, cnf_file):
    if not os.path.exists(cnf_file):
        print("The file %s is not existed!" % cnf_file)
        sys.exit(-1)
    """Update some specified param keys from config file."""
    logger.info("update parameters: %s in conf %s", params, cnf_file)
    config = ConfigParser(allow_no_value=True)
    config.read(cnf_file)

    for section in params:
        if not config.has_section(section):
            config.add_section(section)
        for key in params[section]:
            config.set(section, key, params[section][key])
    config.write(open(cnf_file, "w"))


def build_pgsql_safe_config(outcfg, user):
    if os.path.exists(outcfg):
        raise Exception("pgsql safe config file is already exists.%s" % outcfg)
    pgsafe = dict(pg_safe=dict(user=user))
    write_cnf(pgsafe, None, outcfg)
    return True, 'build pgsql config "%s" successfully!' % outcfg


# cnf file with no section
def write_cnf(params, democfg, outcfg):
    config = ConfigParser(allow_no_value=True)
    if democfg is not None:
        config.read(democfg)
    for section in params:
        if not config.has_section(section):
            config.add_section(section)
        for key in params[section]:
            config.set(section, key, params[section][key])
    out = open(outcfg, "w")
    config.write(out)
    out.close()


def is_share_storage(storage_type):
    return (
        storage_type == STORAGE_TYPE_FC_SAN or storage_type == STORAGE_TYPE_POLAR_STORE
    )
