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

"""
Get kms plain key from central manager
"""
import sys
import requests
import base64
import json

sys.path.append("/scripts")
url = sys.argv[1]
r = requests.get(url)
if r.status_code == 200 and '{"secret":' in r.content:
    key_enckey_json = json.loads(r.content)
    key_enckey = key_enckey_json["secret"]
    print(base64.b64decode(key_enckey))
else:
    print("Get kms key error: %s" % r.content)
    exit(1)
