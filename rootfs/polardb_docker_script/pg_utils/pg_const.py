#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
The dir in the docker
"""
PGDATA = os.getenv("PG_DATA", "/data")
PG_EXTERNAL_DATA = os.getenv("PG_EXTERNAL_DATA ", "/disk1")
LOG = os.getenv("PG_LOG", "/log")
CONF = "/conf"
HBA_CONF_PATH = os.path.join(PGDATA, "pg_hba.conf")

PG_BASE_DIR = os.getenv("POLARDB_BASE_DIR", "/u01/polardbmpd")
PATH = "%s/bin" % PG_BASE_DIR
POSTGRESQL_CONF_DEMO = "/postgresql.conf.demo"
ENV_FILE = "/conf/env_file"
POSTGRES_CONF_PATH = "/data/postgresql.conf"
HBA_CONF = "pg_hba.conf"
POSTGRES_CONF = "postgresql.conf"
RECOVERY_CONF = "recovery.conf"
SSL_CERT = "server.crt"
SSL_KEY = "server.key"
SSL_CERT_PATH = "%s/%s" % (PGDATA, SSL_CERT)
SSL_KEY_PATH = "%s/%s" % (PGDATA, SSL_KEY)

"""
The const when creat instance
"""
POSTGRES_INITDB_ARGS = "--encoding=UTF8 --no-locale  --data-checksums"
PORT_NAME = "access_port"
DB_VERSION = "10.0"
POLAR_INTERNAL_EXTENSIONS = ["polar_vfs", "polar_monitor"]

"""
The log for pg_ctl cmd
"""
START_LOG = "/log/start.log"
RESTART_LOG = "/log/restart.log"
STOP_LOG = "/log/stop.log"
RELOAD_LOG = "/log/reload.log"

DB_TYPE_PGSQL = "pgsql"
DB_TYPE_mpdSQL = "mpdsql"
PGSQL_DB_VERSION = "10.0"

NORMAL_ACCOUNT = 1
ALIYUN_SUPER_ACCOUNT = 6
PRIVILEDGE_TYPE_AURORA = 7
PRIVILEDGE_TYPE_REPLICATE = 15
PRIVILEDGE_TYPE_SUPER = 18

SYSTEM_ACCOUNT_AURORA = "aurora"
RDS_INTERNAL_MARK = "/* rds internal mark */ "

"""
Some default value about log
"""
DEFAULT_LOG_NUM = 5
BIZ_LOGGER_NAME = "dbaas"
DEFAULT_WATCH_PERIOD = 60
MAX_LOGFILE_BYTES = 1024 * 1024 * 10
LOG_FORMAT_STR = "[%(asctime)s] - [%(levelname)s] [%(module)s.%(funcName)s:%(lineno)d] [%(process)d-%(thread)d] --%(message)s"

DEFAULT_INSTALL_INSTANCE_LOG_NAME = os.path.join(LOG, "install_instance.log")
DEFAULT_INSTANCE_TASK_LOG_NAME = os.path.join(LOG, "instance_task.log")

NEED_UPGRADE_POSTGRESQL_CONF_PARAMS = [
    "archive_mode",
    "max_connections",
    "shared_buffers",
    "effective_cache_size",
    "checkpoint_segments",
    "work_mem",
    "maintenance_work_mem",
    "checkpoint_completion_target",
    "max_parallel_workers_per_gather",
    "max_parallel_workers",
    "wal_buffers",
    "default_statistics_target",
    "rds.rds_max_non_super_conns",
    "rds.rds_max_super_conns",
    "max_wal_size",
    "min_wal_size",
    "max_prepared_transactions",
]
INSTALL_INSTANCE_SCRIPT = "/docker_script/pg_tasks/install_instance.py"

"""
The default value of connection
"""
DEFAULT_DB = "postgres"
POLARDBADMIN_DB = "polardb_admin"
DEFAULT_PORT = 3001

"""
ENV name from dbaas
"""
PHYSICAL_INS_ID = "physical_ins_id"

"""
Const for backup info
"""
BACKUP_LABEL_PATH = "/data/backup_label"
BACKUP_LABEL = "backup"

INSTALL = "install"
RESTORE = "restore"

"""
The file store some usefull information, the structure like:
[pg_safe]
user = pg462857
"""
PG_SAFE_CONF = os.path.join(LOG, "pg_safe.cnf")
INS_LOCK_FILE = os.path.join(PGDATA, "ins_lock")
INS_INSTALL_STEP = os.path.join(PGDATA, "ins_install_step")
INS_CTX = os.path.join(PGDATA, "ins_ctx")
SET_INSTALL_STEP_LOCK = os.getenv(
    "PG_SET_INSTALL_STEP_LOCK", "/tmp/set_install_step_lock"
)
INS_LOGIC_ID = os.path.join(PGDATA, "ins_logic_id")

SERVICE_TYPE_RW = "polardb_mpd_rw"
SERVICE_TYPE_RO = "polardb_mpd_ro"
SERVICE_TYPE_STANDBY = "polardb_mpd_standby"
SERVICE_TYPE_DATAMAX = "polardb_mpd_datamax"

SERVICE_TYPE_RW_CODE = 0
SERVICE_TYPE_RO_CODE = 3
SERVICE_TYPE_STANDBY_CODE = 7

LOG_AGENT_DATA_DIR = "/home/pgsql/data"

ALL_LIBRARY_PATHS = [
    os.getenv("LD_LIBRARY_PATH", ""),
    os.path.join(PG_BASE_DIR, "lib"),
    os.path.join(PG_BASE_DIR, "lib/postgresql"),
]

SECRET_ENV_KEYS = {"srv_opr_password", "password"}

INITDB_SUPERUSER = os.environ.get("INITDB_SUPERUSER", "postgres")

ENGINE = "engine"
MANAGER = "manager"
STORAGE_TYPE_LOCAL = "local"
STORAGE_TYPE_POLAR_STORE = "polarstore"
STORAGE_TYPE_FC_SAN = "fcsan"

RESTORE_DOWNLOADS_DIR = os.getenv(
    "RESTORE_DOWNLOADS_DIR", "/home/pgsql/restore/downloads"
)
RESTORE_JOB_STATUS = os.getenv("RESTORE_JOB_STATUS", "/home/pgsql/restore/status")
RESTORE_JOB_WORKER = os.getenv("RESTORE_JOB_WORKER", "/home/pgsql/restore/worker")
RESTORE_JOB_LOG = os.getenv("RESTORE_JOB_LOG", "/home/pgsql/restore/log")

HUGETLB_SHM_GROUP = "root"
PG_LOCK_FILE = "postmaster.pid"
DEFAULT_TDE_FUNCTION_OPT = "-e aes-256"
DEFAULT_TDE_CLUSTER_COMMAND_PREFIX = "python /scripts/tde_get_plain_dk.py"
DEFAULT_TDE_SCRIPT = "/scripts/tde_get_plain_dk.py"

CORE_PATTERN_FILE = "/proc/sys/kernel/core_pattern"
LOG_CORE_DIR = "%s/core" % LOG

DMA_ROLE_MASTER = "master"
DMA_ROLE_FOLLOWER = "follower"
