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
"""Module in charge of managing the dynamic
data created when a Lambda function is invoked."""

import json
import socket
from faassupervisor.utils import SysUtils, FileUtils, StrUtils

def get_function_ip():
    """Returns the IP of the invoked function."""
    return socket.gethostbyname(socket.gethostname())

class LambdaInstance():
    """Stores and manages the Lambda invocation information."""

    PERMANENT_FOLDER = "/var/task"

    def __init__(self, event_info, context):
        self.raw_event = event_info
        self.context = context
        self._set_tmp_folders()
        self._parse_exec_script_and_commands()
        self._set_lambda_env_vars()

    def _set_tmp_folders(self):
        self.input_folder = SysUtils.get_env_var("TMP_INPUT_DIR")
        self.output_folder = SysUtils.get_env_var("TMP_OUTPUT_DIR")

    def _parse_exec_script_and_commands(self):
        # Check for script in function event
        if 'script' in self.raw_event:
            self.script_path = f"{self.input_folder}/script.sh"
            script_content = StrUtils.base64_to_str(self.raw_event['script'])
            FileUtils.create_file_with_content(self.script_path, script_content)
        # Container invoked with arguments
        elif 'cmd_args' in self.raw_event:
            # Add args
            self.cmd_args = json.loads(self.raw_event['cmd_args'])
        # Script to be executed every time (if defined)
        elif SysUtils.is_var_in_env('INIT_SCRIPT_PATH'):
            # Add init script
            self.init_script_path = f"{self.input_folder}/init_script.sh"
            FileUtils.cp_file(SysUtils.get_env_var("INIT_SCRIPT_PATH"), self.init_script_path)

    def _set_lambda_env_vars(self):
        SysUtils.set_env_var('AWS_LAMBDA_REQUEST_ID', self.get_request_id())

    def get_memory(self):
        """Returns the amount of memory available to the function in MB."""
        return int(self.context.memory_limit_in_mb)

    def get_request_id(self):
        """Returns the request id of the function invocation."""
        return self.context.aws_request_id

    def get_function_name(self):
        """Returns the name of the function."""
        return self.context.function_name

    def get_log_group_name(self):
        """Returns the name of the Amazon CloudWatch Logs group for the function."""
        return self.context.log_group_name

    def get_log_stream_name(self):
        """Returns the name of the Amazon CloudWatch Logs stream for the function."""
        return self.context.log_stream_name

    def get_function_arn(self):
        """Returns the invoked function ARN."""
        return self.context.invoked_function_arn

    def get_remaining_time_in_seconds(self):
        """Returns the amount of time remaining for the invocation in seconds."""
        remaining_time = int(self.context.get_remaining_time_in_millis() / 1000)
        timeout_threshold = int(SysUtils.get_env_var('TIMEOUT_THRESHOLD'))
        return remaining_time - timeout_threshold
