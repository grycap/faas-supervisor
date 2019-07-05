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
related with the binary supervisor."""

import subprocess
import sys
from faassupervisor.faas import DefaultSupervisor
from faassupervisor.logger import get_logger
from faassupervisor.utils import SysUtils, FileUtils, StrUtils


class BinarySupervisor(DefaultSupervisor):
    """Supervisor class used in the Binary environment."""

    _SCRIPT_FILE_NAME = 'script.sh'

    def __init__(self):
        get_logger().info('SUPERVISOR: Initializing Binary supervisor')

    def execute_function(self):
        if SysUtils.is_var_in_env('SCRIPT'):
            script_path = SysUtils.join_paths(SysUtils.get_env_var("TMP_INPUT_DIR"),
                                              self._SCRIPT_FILE_NAME)
            script_content = StrUtils.base64_to_str(SysUtils.get_env_var('SCRIPT'))
            FileUtils.create_file_with_content(script_path, script_content)
            get_logger().info("Script file created in '%s'", script_path)
            FileUtils.set_file_execution_rights(script_path)
            get_logger().info("Executing user defined script: '%s'", script_path)
            try:
                pyinstaller_library_path = SysUtils.get_env_var('LD_LIBRARY_PATH')
                orig_library_path = SysUtils.get_env_var('LD_LIBRARY_PATH_ORIG')
                if orig_library_path:
                    SysUtils.set_env_var('LD_LIBRARY_PATH', orig_library_path)
                script_output = subprocess.check_output(['/bin/sh', script_path],
                                                        stderr=subprocess.STDOUT).decode("latin-1")
                SysUtils.set_env_var('LD_LIBRARY_PATH', pyinstaller_library_path)
                get_logger().info(script_output)
            except subprocess.CalledProcessError as cpe:
                # Exit with user script return code if an
                # error occurs (Kubernetes handles the error)
                get_logger().error(cpe.output.decode('latin-1'))
                sys.exit(cpe.returncode)
        else:
            get_logger().error('No user script found!')

    def create_response(self):
        pass

    def create_error_response(self):
        pass
