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

import time
import os

from pg_utils.envs import engine_env
from pg_utils.os_operate import (
    add_os_user,
    del_os_user,
    is_os_user_exists,
    create_core_pattern_dir,
)
from pg_utils.utils import get_initdb_user_uid


def main():
    # add core_pattern dir link
    create_core_pattern_dir()

    # add user if not exist
    logic_ins_id = os.getenv("logic_ins_id")
    initdb_user = engine_env.get_initdb_user()
    initdb_user_uid = get_initdb_user_uid(logic_ins_id)
    if not is_os_user_exists(initdb_user, initdb_user_uid):
        del_os_user(initdb_user, True)
        add_os_user(initdb_user, initdb_user_uid)

    while True:
        time.sleep(86400 * 365)


if __name__ == "__main__":
    main()
