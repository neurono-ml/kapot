#!/bin/bash

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

set -e

RELEASE_FLAG=${RELEASE_FLAG:=release}

./dev/build-kapot-executables.sh

docker compose build

. ./dev/build-set-env.sh
docker build -t "apache/arrow-kapot-standalone:$kapot_VERSION" -f dev/docker/kapot-standalone.Dockerfile .

docker tag kapot-executor "apache/arrow-kapot-executor:$kapot_VERSION"
docker tag kapot-scheduler "apache/arrow-kapot-scheduler:$kapot_VERSION"
docker tag kapot-benchmarks "apache/arrow-kapot-benchmarks:$kapot_VERSION"

docker build -t "apache/arrow-kapot-cli:$kapot_VERSION" -f dev/docker/kapot-cli.Dockerfile .
