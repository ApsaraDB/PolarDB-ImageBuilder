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

import atexit
import logging.handlers
import os
import sys
import uuid

from pg_utils.pg_const import LOG

logger = logging.getLogger("polardb_pg")
logger.setLevel(logging.DEBUG)

request_id = os.getenv("request_id")
if not request_id:
    request_id = str(uuid.uuid4())

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - "
    + request_id
    + " - %(filename)s:%(lineno)d - %(message)s"
)

sh = logging.StreamHandler(sys.stdout)
sh.flush = sys.stdout.flush
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter)
logger.addHandler(sh)

fh = logging.handlers.RotatingFileHandler(
    "%s/manager.log" % LOG, backupCount=15, maxBytes=10 * 1024 * 1024, mode="a"
)
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)
logger.addHandler(fh)

for handler in logger.handlers:
    atexit.register(handler.flush)
