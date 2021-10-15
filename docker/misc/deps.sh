#!/usr/bin/bash
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
# get package latest version from dep file

deps_file="$1"

function usage {
    echo "Usage: ./deps.sh deps_list_file"
}

if [ -z "$deps_file" ]; then
    usage
    exit 1
fi

function get_packages_latest_version {
    error_packages=""
    for package in `cat $deps_file`; do
        info=`yum info -d1 -b current $package 2>&1`
        if [ $? != 0 ]; then
            error_packages="$error_packages $package"
            continue
        fi
        version=`echo "$info" | grep -m1 "Version" | cut -d: -f2 | tr -d '[:space:]'`
        release=`echo "$info" | grep -m1 "Release" | cut -d: -f2 | tr -d '[:space:]'`
        echo "$package-$version-$release"
    done
    if [[ -n $error_packages ]]; then
        echo "[ERROR] can not get version for packages: $error_packages"
    fi
}

get_packages_latest_version
