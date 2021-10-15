#!/usr/bin/env sh
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

if [ ! -f /data/postmaster.pid ]; then
 echo "Not found /data/postmaster.pid"
 exit 0
fi

postmaster_pid=`cat /data/postmaster.pid |head -1`
echo "dump postmaster pid $postmaster_pid"
if [ ! -f /proc/$postmaster_pid/cmdline ]; then
 echo "Not found postmaster_pid: ${postmaster_pid}"
 exit 0
fi

pstack $postmaster_pid > /data/postgres_hang.dump

postgres_ids=`ps --ppid $postmaster_pid|grep postgres|awk '{print $1}'`
echo $postgres_ids

for id in ${postgres_ids}; do
    cmdline=`cat /proc/$id/cmdline`
    echo "kill process ${id}, cmdline: ${cmdline}" >> /data/postgres_hang.dump
    pstack ${id} >> /data/postgres_hang.dump
    kill -9 ${id}
done

# kill postmaster at the last
kill -9 $postmaster_pid

exit 0