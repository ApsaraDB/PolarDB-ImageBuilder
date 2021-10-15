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
This file define action to add/remove ro instance
"""

from pg_tasks.manager_user import create_slot, drop_slot
from pg_utils.envs import engine_env
from pg_utils.pg_const import SERVICE_TYPE_RO


class ReplicaManager:
    def __init__(self, docker_env):
        self.srv_opr_action = docker_env.get("srv_opr_action")

    def do_action(self):
        if self.srv_opr_action == "create_replication":
            create_replication()
        elif self.srv_opr_action == "remove_replication":
            drop_replication()
        else:
            raise Exception(
                "The action %s of task %s do not support"
                % (self.srv_opr_action, "replica")
            )


def create_replication():
    create_slot(
        get_current_operation_slot_name(),
        engine_env.get_initdb_user(),
        engine_env.get_server_port(),
    )


def drop_replication():
    current_operation_slot_name = get_current_operation_slot_name()
    drop_slot(
        current_operation_slot_name,
        engine_env.get_initdb_user(),
        engine_env.get_server_port(),
    )


def get_current_operation_slot_name():
    if engine_env.ins_topology_4_replication:
        service_type = engine_env.get_current_ins_service_type()
        slot_name = engine_env.get_slot_name_by_ins_info(
            service_type, engine_env.current_ins_json
        )
    else:
        # TODO 兼容代码，operator升级后，可以删掉
        slot_name = engine_env.get_slot_name_by_ins_info(
            SERVICE_TYPE_RO, engine_env.ro_custins_current_json
        )
    return slot_name
