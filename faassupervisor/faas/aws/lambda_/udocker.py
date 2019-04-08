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

import os
import subprocess
import socket
import faassupervisor.utils as utils
import faassupervisor.logger as logger

class Udocker():

    container_name = "udocker_container"
    script_exec = "/bin/sh"

    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        self.input_file_path = utils.get_environment_variable("INPUT_FILE_PATH") if utils.is_variable_in_environment("INPUT_FILE_PATH") else ""
        
        utils.create_folder(utils.get_environment_variable("UDOCKER_DIR"))
        
        self.udocker_exec = [utils.get_environment_variable("UDOCKER_EXEC")]
        self.container_output_file = utils.join_paths(utils.get_environment_variable("TMP_OUTPUT_DIR"), "container-stdout.txt")
        
        if utils.is_variable_in_environment("IMAGE_ID"):
            self.container_image_id = utils.get_environment_variable("IMAGE_ID")
            self._set_udocker_commands()
        else:
            logger.get_logger().error("Container image id not specified")
            raise Exception("Container image id not specified.")
    
    def _set_udocker_commands(self):
        self.cmd_get_images = self.udocker_exec + ["images"]
        self.cmd_load_image = self.udocker_exec + ["load", "-i", self.container_image_id]
        self.cmd_download_image = self.udocker_exec + ["pull", self.container_image_id]
        self.cmd_list_containers = self.udocker_exec + ["ps"]
        self.cmd_create_container = self.udocker_exec + ["create", "--name={0}".format(self.container_name), self.container_image_id]
        self.cmd_set_execution_mode = self.udocker_exec + ["setup", "--execmode=F1", self.container_name]
        self.cmd_container_execution = self.udocker_exec + ["--quiet", "run"]
        
    def prepare_container(self):
        self._create_image()
        self._create_container()
        self._create_command()        

    def _create_image(self):
        if self._is_container_image_downloaded():
            logger.get_logger().info("Container image '{0}' already available".format(self.container_image_id))
        else:
            if utils.is_variable_in_environment("IMAGE_FILE"):
                self._load_local_container_image()
            else:
                self._download_container_image()
                
    def _is_container_image_downloaded(self):
        cmd_out = utils.execute_command_and_return_output(self.cmd_get_images)
        return self.container_image_id in cmd_out                

    def _load_local_container_image(self):
        logger.get_logger().info("Loading container image '{0}'".format(self.container_image_id))
        utils.execute_command(self.cmd_load_image)
        
    def _download_container_image(self):
        logger.get_logger().info("Pulling container '{0}' from Docker Hub".format(self.container_image_id))
        utils.execute_command(self.cmd_download_image)

    def _create_container(self):
        if self._is_container_available():
            logger.get_logger().info("Container already available")
        else:
            logger.get_logger().info("Creating container based on image '{0}'.".format(self.container_image_id))
            utils.execute_command(self.cmd_create_container)
        utils.execute_command(self.cmd_set_execution_mode)

    def _is_container_available(self):
        cmd_out = utils.execute_command_and_return_output(self.cmd_list_containers)
        return self.container_name in cmd_out

    def _create_command(self):
        self._add_container_volumes()
        self._add_container_environment_variables()
        # Container running script
        if hasattr(self.lambda_instance, 'script_path'): 
            # Add script in memory as entrypoint
            self.cmd_container_execution += ["--entrypoint={0} {1}".format(self.script_exec, self.lambda_instance.script_path), self.container_name]
        # Container with args
        elif hasattr(self.lambda_instance, 'cmd_args'):
            # Add args
            self.cmd_container_execution += [self.container_name]
            self.cmd_container_execution += self.lambda_instance.cmd_args
        # Script to be executed every time (if defined)
        elif hasattr(self.lambda_instance, 'init_script_path'):
            # Add init script
            self.cmd_container_execution += ["--entrypoint={0} {1}".format(self.script_exec, self.lambda_instance.init_script_path), self.container_name]
        # Only container
        else:
            self.cmd_container_execution += [self.container_name]
    
    def _add_container_volumes(self):
        self.cmd_container_execution.extend(["-v", self.lambda_instance.input_folder])
        self.cmd_container_execution.extend(["-v", self.lambda_instance.output_folder])
        self.cmd_container_execution.extend(["-v", "/dev", "-v", "/proc", "-v", "/etc/hosts", "--nosysdirs"])
        if utils.is_variable_in_environment('EXTRA_PAYLOAD'):
            self.cmd_container_execution.extend(["-v", self.lambda_instance.permanent_folder])

    def _add_container_environment_variables(self):
        self.cmd_container_execution += self._parse_container_environment_variable("REQUEST_ID", self.lambda_instance.request_id)
        self.cmd_container_execution += self._parse_container_environment_variable("INSTANCE_IP", socket.gethostbyname(socket.gethostname()))        
        self.cmd_container_execution += self._get_user_defined_variables()
        self.cmd_container_execution += self._get_iam_credentials()        
        self.cmd_container_execution += self._get_input_file()
        self.cmd_container_execution += self._get_output_dir()
        self.cmd_container_execution += self._get_extra_payload_path()
       
    def _parse_container_environment_variable(self, key, value):
        return ["--env", str(key) + '=' + str(value)] if key and value else []
        
    def _get_user_defined_variables(self):
        result = []
        for key,value in utils._get_user_defined_variables().items():
            result.extend(self._parse_container_environment_variable(key, value))
        return result

    def _get_iam_credentials(self):
        credentials = []
        iam_creds = {'CONT_VAR_AWS_ACCESS_KEY_ID':'AWS_ACCESS_KEY_ID',
                     'CONT_VAR_AWS_SECRET_ACCESS_KEY':'AWS_SECRET_ACCESS_KEY',
                     'CONT_VAR_AWS_SESSION_TOKEN':'AWS_SESSION_TOKEN'}
        # Add IAM credentials
        for key,value in iam_creds.items():
            if utils.is_variable_in_environment(key):
                credentials.extend(self._parse_container_environment_variable(value, utils.get_environment_variable(key)))
        return credentials
    
    def _get_input_file(self):
        return self._parse_container_environment_variable("INPUT_FILE_PATH", self.input_file_path)
    
    def _get_output_dir(self):
        return self._parse_container_environment_variable("TMP_OUTPUT_DIR", self.lambda_instance.output_folder)
            
    def _get_extra_payload_path(self):
        ppath = []
        if utils.is_variable_in_environment('EXTRA_PAYLOAD'):
            ppath += self._parse_container_environment_variable("EXTRA_PAYLOAD", utils.get_environment_variable("EXTRA_PAYLOAD"))
        return ppath
          
    def launch_udocker_container(self):
        remaining_seconds = self.lambda_instance.get_invocation_remaining_seconds()
        logger.get_logger().info("Executing udocker container. Timeout set to {0} seconds".format(remaining_seconds))
        logger.get_logger().debug("Udocker command: {0}".format(self.cmd_container_execution))
        with open(self.container_output_file, "w", encoding="latin-1") as out:
            with subprocess.Popen(self.cmd_container_execution, 
                                  stderr=subprocess.STDOUT, 
                                  stdout=out, 
                                  preexec_fn=os.setsid) as process:
                try:
                    process.wait(timeout=remaining_seconds)
                except subprocess.TimeoutExpired:
                    logger.get_logger().info("Stopping process '{0}'".format(process))
                    process.kill()
                    logger.get_logger().warning("Container timeout")
                    raise
        if os.path.isfile(self.container_output_file):
            return utils.read_file(self.container_output_file, file_encoding="latin-1")
