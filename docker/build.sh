#!/usr/bin/env bash
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

set -e
set -o pipefail

build_version=1.0-SNAPSHOT

base_image_name=polardb_pg/polardb_pg_base

#base_cache_option="--no-cache"
#dev_cache_option="--no-cache"

echo "building ${base_image_name}:${build_version}"
docker build --network=host ${base_cache_option} -t ${base_image_name}:${build_version}  -f Dockerfile.base .

echo "
base image:
${base_image_name}:${build_version}
"