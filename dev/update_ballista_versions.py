#!/usr/bin/env python

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

# Script that updates verions for kapot crates, locally
#
# dependencies:
# pip install tomlkit

import os
import re
import argparse
from pathlib import Path
import tomlkit


def update_cargo_toml(cargo_toml: str, new_version: str):
    print(f'updating {cargo_toml}')
    with open(cargo_toml) as f:
        data = f.read()

    doc = tomlkit.parse(data)
    if "kapot/" in cargo_toml or "kapot-cli/" in cargo_toml:
        doc.get('package')['version'] = new_version

    # kapot crates also depend on each other
    kapot_deps = (
        'kapot',
        'kapot-core',
        'kapot-executor',
        'kapot-scheduler',
        'kapot-cli',
    )
    for kapot_dep in kapot_deps:
        dep = doc.get('dependencies', {}).get(kapot_dep)
        if dep is not None:
            dep['version'] = new_version
        dep = doc.get('dev-dependencies', {}).get(kapot_dep)
        if dep is not None:
            dep['version'] = new_version

    with open(cargo_toml, 'w') as f:
        f.write(tomlkit.dumps(doc))


def update_docker_compose(docker_compose_path: str, new_version: str):
    print(f'Updating kapot versions in {docker_compose_path}')
    with open(docker_compose_path, "r+") as fd:
        data = fd.read()
        pattern = re.compile(r'(^\s+image:\skapot:)\d+\.\d+\.\d+(-SNAPSHOT)?', re.MULTILINE)
        data = pattern.sub(r"\g<1>"+new_version, data)
        fd.truncate(0)
        fd.seek(0)
        fd.write(data)


def main():
    parser = argparse.ArgumentParser(description='Update kapot crate versions.')
    parser.add_argument('new_version', type=str, help='new kapot version')
    args = parser.parse_args()

    repo_root = Path(__file__).parent.parent.absolute()
    kapot_crates = set([
        os.path.join(repo_root, rel_path, "Cargo.toml")
        for rel_path in [
            'kapot-cli',
            'kapot/core',
            'kapot/scheduler',
            'kapot/executor',
            'kapot/client',
            'benchmarks',
            'examples',
        ]
    ])
    new_version = args.new_version

    print(f'Updating kapot versions in {repo_root} to {new_version}')

    for cargo_toml in kapot_crates:
        update_cargo_toml(cargo_toml, new_version)

    for path in (
            "docker-compose.yml",
    ):
        path = os.path.join(repo_root, path)
        update_docker_compose(path, new_version)


if __name__ == "__main__":
    main()
