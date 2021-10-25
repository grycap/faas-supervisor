# Copyright (C) GRyCAP - I3M - UPV
#
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
# limitations under the License.
"""In this module are defined all the methods and classes used
to manage a udocker container in the lambda environment."""

import subprocess
from faassupervisor.exceptions import ContainerImageNotFoundError
from faassupervisor.utils import SysUtils, FileUtils, ConfigUtils
from faassupervisor.logger import get_logger
from faassupervisor.exceptions import ContainerTimeoutExpiredWarning
from faassupervisor.faas.aws_lambda.function import get_function_ip


def _parse_cont_env_var(key, value):
    return ["--env", str(key) + '=' + str(value)] if key and value else []


class Udocker():
    """Class in charge of managing the udocker binary."""

    _CONTAINER_OUTPUT_FILE = SysUtils.join_paths(FileUtils.get_tmp_dir(), "container-stdout")
    _CONTAINER_NAME = "udocker_container"
    _SCRIPT_EXEC = "/bin/sh"

    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        # Create required udocker folder
        FileUtils.create_folder(SysUtils.get_env_var("UDOCKER_DIR"))
        # Init the udocker command that will be executed
        self.udocker_exec = [SysUtils.get_env_var("UDOCKER_EXEC")]
        self.cont_cmd = self.udocker_exec + ["--quiet", "run"]

        self.cont_img_id = ConfigUtils.read_cfg_var('container').get('image')
        if not self.cont_img_id:
            raise ContainerImageNotFoundError()

    def _list_udocker_images_cmd(self):
        return self.udocker_exec + ["images"]

    def _load_udocker_image_cmd(self):
        return self.udocker_exec + ["load", "-i", self.cont_img_id]

    def _download_udocker_image_cmd(self):
        return self.udocker_exec + ["pull", self.cont_img_id]

    def _list_udocker_containers_cmd(self):
        return self.udocker_exec + ["ps"]

    def _create_udocker_container_cmd(self):
        return self.udocker_exec + ["create", f"--name={self._CONTAINER_NAME}", self.cont_img_id]

    def _set_udocker_container_execution_mode_cmd(self):
        return self.udocker_exec + ["setup", "--execmode=F1", self._CONTAINER_NAME]

    def _is_container_image_downloaded(self):
        cmd_out = SysUtils.execute_cmd_and_return_output(self._list_udocker_images_cmd())
        return self.cont_img_id in cmd_out

    def _load_local_container_image(self):
        get_logger().info("Loading container image '%s'", self.cont_img_id)
        SysUtils.execute_cmd(self._load_udocker_image_cmd())

    def _download_container_image(self):
        get_logger().info("Pulling container '%s' from Docker Hub", self.cont_img_id)
        SysUtils.execute_cmd(self._download_udocker_image_cmd())

    def _is_container_available(self):
        cmd_out = SysUtils.execute_cmd_and_return_output(self._list_udocker_containers_cmd())
        return self._CONTAINER_NAME in cmd_out

    def _create_image(self):
        if self._is_container_image_downloaded():
            get_logger().info("Container image '%s' already available", self.cont_img_id)
        else:
            if SysUtils.is_var_in_env("IMAGE_FILE"):
                self._load_local_container_image()
            else:
                self._download_container_image()

    def _create_container(self):
        if self._is_container_available():
            get_logger().info("Container already available")
        else:
            get_logger().info("Creating container based on image '%s'.", self.cont_img_id)
            SysUtils.execute_cmd(self._create_udocker_container_cmd())
        SysUtils.execute_cmd(self._set_udocker_container_execution_mode_cmd())

    def _create_command(self):
        self._add_container_volumes()
        self._add_container_environment_variables()
        # Container running script
        if hasattr(self.lambda_instance, 'script_path'):
            # Add script in memory as entrypoint
            self.cont_cmd += [(f"--entrypoint={self._SCRIPT_EXEC} "
                               f"{self.lambda_instance.script_path}"),
                              self._CONTAINER_NAME]
        # Container with args
        elif hasattr(self.lambda_instance, 'cmd_args'):
            # Add args
            self.cont_cmd += [self._CONTAINER_NAME]
            self.cont_cmd += self.lambda_instance.cmd_args
        # Script to be executed every time (if defined)
        elif hasattr(self.lambda_instance, 'init_script_path'):
            # Add init script
            self.cont_cmd += [(f"--entrypoint={self._SCRIPT_EXEC} "
                               f"{self.lambda_instance.init_script_path}"),
                              self._CONTAINER_NAME]
        # Only container
        else:
            self.cont_cmd += [self._CONTAINER_NAME]

    def _add_container_volumes(self):
        self.cont_cmd.extend(["-v", SysUtils.get_env_var("TMP_INPUT_DIR")])
        self.cont_cmd.extend(["-v", SysUtils.get_env_var("TMP_OUTPUT_DIR")])
        self.cont_cmd.extend(["-v", "/dev", "-v", "/proc", "-v", "/etc/hosts", "--nosysdirs"])
        if SysUtils.is_var_in_env('EXTRA_PAYLOAD'):
            self.cont_cmd.extend(["-v", self.lambda_instance.PERMANENT_FOLDER])

    def _add_cont_env_vars(self):
        for key, value in SysUtils.get_cont_env_vars().items():
            self.cont_cmd.extend(_parse_cont_env_var(key, value))

    def _add_input_file(self):
        self.cont_cmd.extend(_parse_cont_env_var("INPUT_FILE_PATH",
                                                 SysUtils.get_env_var("INPUT_FILE_PATH")))

    def _add_output_dir(self):
        self.cont_cmd.extend(_parse_cont_env_var("TMP_OUTPUT_DIR",
                                                 SysUtils.get_env_var("TMP_OUTPUT_DIR")))

    def _add_event_vars(self):
        self.cont_cmd.extend(_parse_cont_env_var("STORAGE_OBJECT_KEY",
                                                 SysUtils.get_env_var("STORAGE_OBJECT_KEY")))
        self.cont_cmd.extend(_parse_cont_env_var("EVENT_TIME",
                                                 SysUtils.get_env_var("EVENT_TIME")))
        self.cont_cmd.extend(_parse_cont_env_var("EVENT",
                                                 SysUtils.get_env_var("EVENT")))

    def _add_extra_payload_path(self):
        self.cont_cmd.extend(_parse_cont_env_var("EXTRA_PAYLOAD",
                                                 SysUtils.get_env_var("EXTRA_PAYLOAD")))

    def _add_function_request_id(self):
        self.cont_cmd.extend(_parse_cont_env_var("REQUEST_ID",
                                                 self.lambda_instance.get_request_id()))

    def _add_aws_access_keys(self):
        self.cont_cmd.extend(_parse_cont_env_var("AWS_ACCESS_KEY_ID",
                                                 SysUtils.get_env_var("AWS_ACCESS_KEY_ID")))
        self.cont_cmd.extend(_parse_cont_env_var("AWS_SECRET_ACCESS_KEY",
                                                 SysUtils.get_env_var("AWS_SECRET_ACCESS_KEY")))
        self.cont_cmd.extend(_parse_cont_env_var("AWS_SESSION_TOKEN",
                                                 SysUtils.get_env_var("AWS_SESSION_TOKEN")))

    def _add_function_ip(self):
        self.cont_cmd.extend(_parse_cont_env_var("INSTANCE_IP", get_function_ip()))

    def _add_container_environment_variables(self):
        self._add_function_request_id()
        self._add_function_ip()
        self._add_aws_access_keys()
        self._add_cont_env_vars()
        self._add_input_file()
        self._add_output_dir()
        self._add_event_vars()
        self._add_extra_payload_path()

    def prepare_container(self):
        """Prepares the environment to execute the udocker container."""
        self._create_image()
        self._create_container()
        self._create_command()

    def launch_udocker_container(self):
        """Launches the udocker container.
        If the execution time of the container exceeds the defined execution time,
        the container is killed and a warning is raised."""
        remaining_seconds = self.lambda_instance.get_remaining_time_in_seconds()
        get_logger().info("Executing udocker container. Timeout set to '%d' seconds",
                          remaining_seconds)
        get_logger().debug("Udocker command: '%s'", self.cont_cmd)
        with open(self._CONTAINER_OUTPUT_FILE, "wb") as out:
            with subprocess.Popen(self.cont_cmd,
                                  stderr=subprocess.STDOUT,
                                  stdout=out,
                                  start_new_session=True) as process:
                try:
                    process.wait(timeout=remaining_seconds)
                except subprocess.TimeoutExpired:
                    get_logger().info("Stopping process '%s'", process)
                    process.kill()
                    raise ContainerTimeoutExpiredWarning()
        udocker_output = b''
        if FileUtils.is_file(self._CONTAINER_OUTPUT_FILE):
            udocker_output = FileUtils.read_file(self._CONTAINER_OUTPUT_FILE, file_mode="rb")
        return udocker_output
