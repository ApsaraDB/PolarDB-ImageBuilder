#!/usr/bin/env python
# -*- coding: utf-8 -*-
# coding=utf-8
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

import json
import os

from pg_utils.pg_const import (
    PGSQL_DB_VERSION,
    DB_TYPE_PGSQL,
    PG_BASE_DIR,
    SERVICE_TYPE_RO,
    SERVICE_TYPE_RW,
    SERVICE_TYPE_STANDBY,
    SERVICE_TYPE_DATAMAX,
    INITDB_SUPERUSER,
    STORAGE_TYPE_POLAR_STORE,
    STORAGE_TYPE_FC_SAN,
)

MIN_UID = 10000


class EngineImageEnv(object):
    def __init__(self):
        self.port = os.getenv("port")
        self.access_port = os.getenv("access_port")
        self.ins_id = os.getenv("ins_id")
        self.db_type = os.getenv("db_type", DB_TYPE_PGSQL)
        self.db_version = os.getenv("db_version", PGSQL_DB_VERSION)
        self.cluster_custins_info = os.getenv("cluster_custins_info")
        if not self.cluster_custins_info or self.cluster_custins_info == '""':
            self.cluster_custins_info_json = {}
        else:
            self.cluster_custins_info_json = json.loads(self.cluster_custins_info)
        self.srv_opr_action = os.getenv("srv_opr_action")
        self.srv_opr_type = os.getenv("srv_opr_type")
        self.write_recovery_conf = os.getenv("write_recovery_conf")
        self.mem_size = os.getenv("mem_size")
        self.logic_ins_id = os.getenv("logic_ins_id")
        self.service_type = os.getenv("service_type")
        self.standby_rebuild_type = os.getenv("standby_rebuild_type")
        self.mycnf_dict = os.getenv("mycnf_dict", "")
        self.srv_opr_host_ip = json.loads(os.getenv("srv_opr_host_ip", "{}"))
        self.password = os.getenv("srv_opr_password")
        self.pg_base_dir = os.getenv("PG_BASE_DIR", PG_BASE_DIR)
        self.pg_base_bin_dir = "%s/bin" % self.pg_base_dir
        self.polarfs_host_id = os.getenv("polarfs_host_id")
        self.cust_ins_id = os.getenv("cust_ins_id")
        self.shutdown_mode = os.getenv("shutdown_mode", "fast")
        self.shutdown_cleanup = os.getenv("shutdown_cleanup", "false") == "true"
        self.shutdown_timeout = os.getenv("shutdown_timeout", 300)
        self.base_collect_path = os.getenv("base_collect_path")
        self.pod_collect_path = os.getenv("pod_collect_path")
        self.ins_name = os.getenv("ins_name")
        self.slot_unique_name = os.getenv("slot_unique_name", "")
        self.container_mount_path = os.getenv("container_mount_path", "")
        self.host_mount_path = os.getenv("host_mount_path", "")
        self.storage_type = os.getenv("storage_type", STORAGE_TYPE_POLAR_STORE)
        self.reload_instance = os.getenv("reload_instance", "True") == "True"
        self.on_pfs = os.getenv("on_pfs", "True") == "True"
        self.san_device_name = os.getenv("san_device_name", "")

        # 三节点
        self.dma_role = os.getenv("dma_role", "")
        self.primary_system_identifier = os.getenv("primary_system_identifier", "")
        self.recovery_conf = json.loads(os.getenv("recovery_conf", "{}"))

        # DataMax
        self.primary_system_identifier = os.getenv("primary_system_identifier", "")

        # PITR related
        self.pitr_time = os.getenv("pitr_time", "")
        self.restore_job_env = json.loads(os.getenv("restore_job_env", "{}"))
        self.pitr_fetch_logs_env = json.loads(os.getenv("pitr_fetch_logs_env", "{}"))
        self.lock_install_ins = os.getenv("lock_install_ins", "False")

        self.ro_custins_current = os.getenv("ro_custins_current")
        if self.ro_custins_current:
            self.ro_custins_current_json = json.loads(self.ro_custins_current)

        self.ro_custins_all = os.getenv("ro_custins_all")
        if self.ro_custins_all:
            self.ro_custins_all_json = json.loads(self.ro_custins_all)

        self.current_ins = os.getenv("current_ins")
        if self.current_ins:
            self.current_ins_json = json.loads(self.current_ins)

        self.ins_topology_4_replication = os.getenv("ins_topology_4_replication")
        if self.ins_topology_4_replication:
            self.ins_topology_4_replication_json = json.loads(
                self.ins_topology_4_replication
            )

        self.custins_id = os.getenv("custins_id")
        if self.custins_id is None:
            self.custins_id = self.cust_ins_id

        ro_insts = self.cluster_custins_info_json.get(SERVICE_TYPE_RO)
        self.is_standby_ro = True
        if ro_insts:
            ro_inst_ids = ro_insts.keys()
            if self.is_polardb_pg_ro() and self.cust_ins_id in ro_inst_ids:
                self.is_standby_ro = False

        standbys = self.cluster_custins_info_json.get(SERVICE_TYPE_STANDBY)
        self.is_clustered_standby = False
        if standbys:
            if SERVICE_TYPE_RW in standbys.values()[0]:
                self.is_clustered_standby = True
        self.is_tde_enable = os.getenv("tde_enable", "false") == "true"
        if self.is_tde_enable:
            self.tde_attribute = json.loads(os.getenv("tde_attribute"))
            self.secret_get = self.tde_attribute["secret_get"]

        self.create_tablespace_env = json.loads(
            os.getenv("create_tablespace_env", "{}")
        )

    @staticmethod
    def is_engine_type_on_pangu(engine_type):
        return "pangu" == engine_type

    @staticmethod
    def is_engine_type_on_san(engine_type):
        return "san" == engine_type

    @staticmethod
    def is_engine_type_on_polarstore(engine_type):
        return "polarstore" == engine_type

    def is_polardb_pg_rw(self):
        return SERVICE_TYPE_RW == self.service_type

    def is_polardb_pg_ro(self):
        return SERVICE_TYPE_RO == self.service_type

    def is_polardb_pg_standby(self):
        return SERVICE_TYPE_STANDBY == self.service_type

    def is_polardb_pg_datamax(self):
        return SERVICE_TYPE_DATAMAX == self.service_type

    def get_srv_opr_host_ip(self):
        return self.srv_opr_host_ip

    def get_server_port(self):
        return int(self.get_srv_opr_host_ip().get("access_port")[0])

    def get_standby_rebuild_type(self):
        return self.standby_rebuild_type

    def get_inst_attr(self, attr):
        if not self.cluster_custins_info_json:
            raise Exception("failed to get cluster custins info")

        if not self.is_clustered_standby:
            return self.cluster_custins_info_json.get(self.service_type, {}).get(
                self.cust_ins_id, {}
            )[attr]

        if self.is_polardb_pg_rw():
            return self.cluster_custins_info_json.get(SERVICE_TYPE_RW, {}).get(
                self.cust_ins_id, {}
            )[attr]

        if self.is_polardb_pg_ro():
            if self.is_standby_ro:
                for standby_cluster in self.cluster_custins_info_json.get(
                    SERVICE_TYPE_STANDBY
                ).values():
                    inst = standby_cluster.get(SERVICE_TYPE_RO, {}).get(
                        self.cust_ins_id
                    )
                    if inst:
                        return inst[attr]
            else:
                return self.cluster_custins_info_json.get(SERVICE_TYPE_RO, {}).get(
                    self.cust_ins_id, {}
                )[attr]

        if self.is_polardb_pg_standby():
            standby = self.cluster_custins_info_json.get(SERVICE_TYPE_STANDBY, {}).get(
                self.cust_ins_id
            )
            if standby:
                return standby.get(SERVICE_TYPE_RW).get(self.cust_ins_id)[attr]

    def get_polar_storage_params(self):
        if self.storage_type == STORAGE_TYPE_POLAR_STORE:
            pbd_number = self.get_pbd_number()
            pbd_data_version = self.get_data_version()
            polar_disk_name = "/%s-%s" % (pbd_number, pbd_data_version)
            polar_datadir = "/%s-%s/data" % (pbd_number, pbd_data_version)
        elif self.storage_type == STORAGE_TYPE_FC_SAN:
            # /dev/mapper/yunjia-pfs-test-pvc
            san_device_name = self.san_device_name.strip()
            if not san_device_name.startswith("/dev/"):
                raise Exception("polar_disk_name must be start with /dev/")
            polar_disk_name = san_device_name[1:].split("/", 1)[1].replace("/", "_")
            polar_datadir = "/%s/data" % polar_disk_name
        else:
            raise Exception("Unknown storage type: %s" % self.storage_type)

        return polar_disk_name, polar_datadir

    def get_pbd_number(self):
        pbd_list = self.get_inst_attr(attr="pbd_list")
        return pbd_list[0]["pbd_number"]

    def get_data_version(self):
        pbd_list = self.get_inst_attr(attr="pbd_list")
        return pbd_list[0]["data_version"]

    def get_pls_prefix(self):
        pbd_number = self.get_pbd_number()
        pbd_data_version = self.get_data_version()
        pls_prefix = "%s-%s" % (pbd_number, pbd_data_version)
        return pls_prefix

    def get_access_port_from_port(self):
        port_struct = json.loads(self.port)
        return port_struct[self.ins_id]["access_port"][0]

    def get_primary_insts(self):
        primary_insts = {}
        if self.is_polardb_pg_standby():
            primary_insts = self.cluster_custins_info_json.get(SERVICE_TYPE_RW)
        elif self.is_polardb_pg_ro():
            if self.is_standby_ro:
                for standby_cluster in self.cluster_custins_info_json.get(
                    SERVICE_TYPE_STANDBY
                ).values():
                    if self.cust_ins_id in standby_cluster.get(SERVICE_TYPE_RO, {}):
                        return standby_cluster.get(SERVICE_TYPE_RW, {})
            else:
                primary_insts = self.cluster_custins_info_json.get(SERVICE_TYPE_RW, {})

        return primary_insts

    def get_primary_ins_info(self):
        primary_insts = self.get_primary_insts()
        return primary_insts.values()[0]["hostins"].values()[0]

    def get_primary_accounts(self):
        return self.get_primary_insts().values()[0]["accounts"]

    def get_primary_account_by_privilege_type(self, privilege_type):
        target_accounts = []
        accounts = self.get_primary_accounts()
        for acc, acc_info in accounts.iteritems():
            if acc_info["priviledge_type"] == privilege_type:
                target_accounts.append(acc_info)
        return target_accounts

    def get_current_ins_service_type(self):
        custins_id = self.current_ins_json.get("custins_id")
        for service_type, ins_map in self.ins_topology_4_replication_json.items():
            for ins_id, ins_info in ins_map.items():
                if ins_id == custins_id:
                    return service_type

    @staticmethod
    def get_initdb_user():
        return INITDB_SUPERUSER

    def get_envs(self):
        for k, v in os.environ.items():
            if k not in self.__dict__:
                self.__dict__[k] = v
        return self.__dict__

    def get_slot_names_by_service_type(self, service_type):
        slot_names = []
        if self.ins_topology_4_replication:
            for ins_info in self.ins_topology_4_replication_json.get(
                service_type
            ).values():
                slot_names.append(
                    self.get_slot_name_by_ins_info(service_type, ins_info)
                )
        else:
            for ins_info in engine_env.ro_custins_all_json.values():
                slot_names.append(
                    self.get_slot_name_by_ins_info(service_type, ins_info)
                )

        return slot_names

    def get_slot_name_by_ins_info(self, service_type, ins_info):
        return self.get_slot_name(
            service_type, ins_info.get("custins_id"), ins_info.get("slot_unique_name")
        )

    def is_write_recovery_conf(self):
        if self.write_recovery_conf == "true":
            return True
        return False

    @staticmethod
    def get_slot_name(service_type, custins_id, slot_unique_name):
        if service_type == SERVICE_TYPE_STANDBY:
            prefix = "standby"
        elif service_type in [SERVICE_TYPE_RW, SERVICE_TYPE_RO]:
            prefix = "replica"
        else:
            raise Exception("unknown service_type: %s" % service_type)

        if slot_unique_name:
            slot_unique_name = slot_unique_name.replace("-", "_")

        return "%s_%s_%s" % (prefix, custins_id, slot_unique_name)


engine_env = EngineImageEnv()
