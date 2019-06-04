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
"""Module with all the classes and methods
related with the AWS Batch supervisor."""

from faassupervisor.faas.default import DefaultSupervisor
from faassupervisor.logger import get_logger
from faassupervisor.utils import SysUtils, FileUtils, StrUtils


class BatchSupervisor(DefaultSupervisor):
    """Supervisor class used in the Batch environment."""

    _SCRIPT_FILE_NAME = 'script.sh'

    def __init__(self):
        get_logger().info('SUPERVISOR: Initializing AWS Batch supervisor')

    def parse_input(self):
        """Creates script file based on the 'SCRIPT' environment variable."""
        if SysUtils.is_var_in_env('SCRIPT'):
            script_path = SysUtils.join_paths(SysUtils.get_env_var("TMP_INPUT_DIR"),
                                              self._SCRIPT_FILE_NAME)
            script_content = StrUtils.base64_to_str(SysUtils.get_env_var('SCRIPT'))
            FileUtils.create_file_with_content(script_path, script_content)
            get_logger().info("Script file created in '%s'", script_path)
            FileUtils.set_file_execution_rights(script_path)
        else:
            get_logger().error('No user script found!')

    def execute_function(self):
        pass

    def create_response(self):
        pass

    def create_error_response(self):
        pass
