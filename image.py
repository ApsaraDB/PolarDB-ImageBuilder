#!/usr/bin/python
# coding: utf-8
#
# image.py
#   Build polardb pg docker images
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
# See the License for the specific language governing permissions and
#

import argparse
import json
import logging
import os
import subprocess
import sys
import shutil
import yaml
import datetime
import re

engine_images = {}
manager_images = {}

root_dir = os.getcwd()

logger = logging.getLogger("polardb_pg")
logger.setLevel(logging.DEBUG)

formatter = logging.Formatter(
    "%(asctime)s - %(levelname)s - %(filename)s:%(lineno)d - %(message)s"
)

sh = logging.StreamHandler(sys.stdout)
sh.flush = sys.stdout.flush
sh.setLevel(logging.DEBUG)
sh.setFormatter(formatter)
logger.addHandler(sh)

parser = argparse.ArgumentParser(description="build polardb pg and polardb mpd images")
parser.add_argument(
    "-f",
    "--image_config",
    required=False,
    default="./image.yml",
    help="image config file",
)
parser.add_argument(
    "-n",
    "--no_cache",
    action="store_true",
    default=False,
    help="enable docker build cache",
)

today = datetime.datetime.now()

pfsd_rpm = ""

code_branch_pattern = r"CodeBranch:\s+(\S+?)\s+"
pfsd_pattern = r"PFSDVersion:\s+(\S+?)\s+"


class Image:
    def __init__(self, config):
        self.id = config.get("id")
        self.type = config.get("type")

        self.build_image_name = config.get("build_image_name")
        image_name_parts = self.build_image_name.rsplit(":", 1)
        self.build_image_repo = image_name_parts[0]
        if len(image_name_parts) > 1:
            self.build_image_tag = image_name_parts[1]
        else:
            self.build_image_tag = ""
        self.build_image_dockerfile = config.get("build_image_dockerfile")

        self.engine_repo = config.get("engine_repo", "")
        self.engine_branch = config.get("engine_branch", "")
        self.engine_config_template = config.get("engine_config_template", "")
        self.engine_release_date = config.get(
            "engine_release_date", today.strftime("%Y%m%d")
        )

        self.engine_image_id = config.get("engine_image_id", "")
        self.push = config.get("push", True)
        self.enable = config.get("enable", True)

        self.build_image_release_name = ""
        self.engine_source_relative_dir = "polardb_pg-%s" % self.engine_branch
        self.engine_source_dir = os.path.join(root_dir, self.engine_source_relative_dir)

        self.polardb_rpm = config.get("polardb_rpm", "")
        self.pfsd_rpm = config.get("pfsd_rpm", "")

    def set_engine_branch(self, engine_branch):
        self.engine_branch = engine_branch
        self.engine_source_relative_dir = "polardb_pg-%s" % self.engine_branch
        self.engine_source_dir = os.path.join(root_dir, self.engine_source_relative_dir)


def exec_command(command, cwd=None):
    logger.info("Execute command: %s", command)
    try:
        p = subprocess.Popen(command, cwd=cwd, shell=True, stdout=subprocess.PIPE)
        output = p.communicate()[0]
        if p.returncode != 0:
            raise Exception(
                "execute command failed, command: %s, return code %s"
                % (command, p.returncode)
            )
        if output:
            output = output.strip()
        return output
    except Exception as e:
        logger.exception(e)
        raise e


def exec_command_verbose(command, cwd=None):
    logger.info("Execute command: %s", command)
    result = []
    try:
        p = subprocess.Popen(
            command, cwd=cwd, shell=True, stdout=subprocess.PIPE, bufsize=1
        )
        for line in iter(p.stdout.readline, b""):
            sys.stdout.write(line)
            sys.stdout.flush()
            result.append(line)
        p.wait()
        if p.returncode != 0:
            raise Exception(
                "execute command failed, command: %s, return code %s"
                % (command, p.returncode)
            )
    except Exception as e:
        logger.exception(e)
        raise e

    return "".join(result).strip()


def get_git_global_config(key):
    command = "git config --global --get %s" % key
    return exec_command(command)


def get_git_current_commit_id(repo_path):
    command = "git rev-parse HEAD"
    return exec_command(command, cwd=repo_path)


def get_git_current_branch_name(repo_path):
    command = "git branch | grep \\* | cut -d ' ' -f2"
    return exec_command(command, cwd=repo_path)


def get_git_current_repo_url(repo_path):
    command = "git remote -v | grep fetch | cut -f2 | cut -d ' ' -f1"
    return exec_command(command, cwd=repo_path)


def clone_and_checkout(image):
    clone_command = "git clone %s --branch %s --depth 1 %s" % (
        image.engine_repo,
        image.engine_branch,
        image.engine_source_dir,
    )
    exec_command(clone_command)


def submodule_init(image):
    init_command = "git -C %s submodule update --init" % image.engine_source_dir
    exec_command(init_command)

def docker_build_base_image():
    logger.info("build base image...")
    bulid_base_command = "./build.sh"
    exec_command_verbose(bulid_base_command, cwd=os.path.join(root_dir, "docker"))

def docker_build_engine_image(image, args):
    if os.path.exists(image.engine_source_dir):
        shutil.rmtree(image.engine_source_dir)
    os.makedirs(image.engine_source_dir)
    clone_and_checkout(image)
    submodule_init(image)

    # 拷贝内核参数模板到指定位置
    config_template_path = os.path.join(
        image.engine_source_dir, image.engine_config_template
    )
    if os.path.exists(config_template_path):
        copy_command = "cp %s rootfs/%s" % (
            config_template_path,
            os.path.basename(config_template_path),
        )
        exec_command(copy_command)
    else:
        raise Exception(
            "Can not find engine config template: %s" % image.engine_config_template
        )

    kernel_repo_url = get_git_current_repo_url(image.engine_source_dir)
    kernel_repo_branch = get_git_current_branch_name(image.engine_source_dir)
    kernel_repo_commit = get_git_current_commit_id(image.engine_source_dir)

    current_repo_url = get_git_current_repo_url(root_dir)
    current_repo_branch = get_git_current_branch_name(root_dir)
    current_repo_commit = get_git_current_commit_id(root_dir)

    # engine image tag: pg_major.pg_minor.polar_release_date.commit_id.build_time
    result = exec_command(
        "grep -hw 'PACKAGE_VERSION=' %s"
        % os.path.join(image.engine_source_dir, "configure")
    ).strip()
    if not result:
        raise Exception("Can not find engine PACKAGE_VERSION")
    parts = result.split("=")[1].strip("'")
    pacakge_version = "%s" % (parts)
    #result = exec_command("grep -h -A1 '&polar_release_date' %s | tail -n 1" % (os.path.join(image.engine_source_dir, "src/backend/utils/misc/guc.c"))).strip()
    #if not result:
    #    raise Exception("Can not find polar_release_date")
    polar_release_date = "20210910"
    image_tag = image.build_image_tag
    if not image_tag:
        image_tag = "%s.%s.%s.%s" % (
            pacakge_version,
            polar_release_date,
            kernel_repo_commit[:8],
            today.strftime("%Y%m%d%H%M%S"),
        )

    user_email = get_git_global_config("user.email")
    cache_option = ""
    if args.no_cache:
        cache_option = "--no-cache"
    image_release_name = "%s:%s" % (image.build_image_repo, image_tag)
    image.build_image_release_name = image_release_name

    build_command = " ".join(
        [
            "docker build %s --network=host -t %s "
            % (cache_option, image_release_name),
            "--build-arg CodeSource=%s" % current_repo_url,
            "--build-arg CodeBranch=%s" % current_repo_branch,
            "--build-arg CodeVersion=%s" % current_repo_commit,
            "--build-arg PolarSource=%s" % kernel_repo_url,
            "--build-arg PolarBranch=%s" % kernel_repo_branch,
            "--build-arg PolarVersion=%s" % kernel_repo_commit,
            "--build-arg POLAR_SOURCE_DIR=%s" % image.engine_source_relative_dir,
            "--build-arg BuildBy=%s" % user_email,
            "--build-arg PFSRPM=%s" % pfsd_rpm,
            "--build-arg PolarDBRPM=%s" % image.polardb_rpm,
            "-f %s ." % image.build_image_dockerfile,
        ]
    )

    exec_command_verbose(build_command)

    if image.push:
        docker_push(image_release_name)

    if os.path.exists(image.engine_source_dir):
        shutil.rmtree(image.engine_source_dir)

    return image_release_name


def docker_build_manager_image(image, args):
    current_repo_url = get_git_current_repo_url(root_dir)
    current_repo_branch = get_git_current_branch_name(root_dir)
    current_repo_commit = get_git_current_commit_id(root_dir)

    user_email = get_git_global_config("user.email")

    kernel_image = engine_images.get(image.engine_image_id)
    if not kernel_image:
        raise Exception("Can't find engine image %s" % image.engine_image_id)

    cache_option = ""
    if args.no_cache:
        cache_option = "--no-cache"

    image_tag = image.build_image_tag
    if not image_tag:
        image_tag = "%s.%s" % (today.strftime("%Y%m%d%H%M%S"), current_repo_commit[:8])
    image_release_name = "%s:%s" % (image.build_image_repo, image_tag)

    build_command = " ".join(
        [
            "docker build %s --network=host -t %s" % (cache_option, image_release_name),
            "--build-arg CodeSource=%s" % current_repo_url,
            "--build-arg CodeBranch=%s" % current_repo_branch,
            "--build-arg CodeVersion=%s" % current_repo_commit,
            "--build-arg BuildBy=%s" % user_email,
            "--build-arg ENGINE_IMAGE_FULL_NAME=%s"
            % kernel_image.build_image_release_name,
            "-f %s ." % image.build_image_dockerfile,
        ]
    )
    exec_command_verbose(build_command)

    if image.push:
        docker_push(image_release_name)

    return image_release_name


def docker_push(image_name):
    push_command = "docker push %s" % image_name
    exec_command_verbose(push_command)


def wget_and_rpm_info(rpm_url):
    package = rpm_url.split("/")[-1]
    wget_command = "rm -f %s && wget %s" % (package, rpm_url)
    exec_command_verbose(wget_command)
    rpm_info_command = "rpm -pqi ./%s" % package
    code_branch = ""
    pfsd = ""
    output = exec_command(rpm_info_command)
    code_branch_obj = re.search(code_branch_pattern, output, re.M | re.I)
    if code_branch_obj is not None:
        code_branch = code_branch_obj.groups()[0]
    pfsd_obj = re.search(pfsd_pattern, output, re.M | re.I)
    if pfsd_obj is not None:
        pfsd = pfsd_obj.groups()[0]
    return (code_branch, pfsd)


def main():
    global pfsd_rpm
    args = parser.parse_args()
    with open(args.image_config) as fd:
        config = yaml.safe_load(fd)
        result_file = config.get("result", "./image.out")
        pfsd_rpm = config.get("pfsd_rpm", "bash")
        ids = set()
        for item in config.get("images", []):
            image = Image(item)
            if not image.enable:
                logger.info("Ignore disabled image %s", image.id)
                continue

            if image.id in ids:
                raise Exception("Found duplicate image id: %s" % image.id)
            ids.add(image.id)

            if image.polardb_rpm != "":
                (engine_branch, pfsd) = wget_and_rpm_info(image.polardb_rpm)
                image.set_engine_branch(engine_branch)

            if image.type == "manager":
                manager_images[image.id] = image
            elif image.type == "engine":
                engine_images[image.id] = image

    summary = []
    all_images = []
    all_images.extend(engine_images.values())
    all_images.extend(manager_images.values())

    docker_build_base_image()
    
    prompt_info = json.dumps([image.build_image_name for image in all_images], indent=4, sort_keys=True)
    logger.info("Will build images: %s", prompt_info)

    for image in all_images:
        if image.type == "engine":
            image_name = docker_build_engine_image(image, args)
        elif image.type == "manager":
            image_name = docker_build_manager_image(image, args)
        else:
            raise Exception("Unknown image type: %s" % image)

        summary.extend([image_name])

    for item in summary:
        logger.info("Successfully build image: %s", item)

    with open(result_file, "w") as fd:
        for image in summary:
            fd.write("%s\n" % image)


if __name__ == "__main__":
    main()
