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
    _OSCAR_SCRIPT_PATH = '/oscar/config/script.sh'

    def __init__(self):
        self.output = ''
        get_logger().info('SUPERVISOR: Initializing Binary supervisor')

    def _get_script_path(self):
        script_path = None
        if SysUtils.is_var_in_env('SCRIPT'):
            script_path = SysUtils.join_paths(SysUtils.get_env_var("TMP_INPUT_DIR"),
                                              self._SCRIPT_FILE_NAME)
            script_content = StrUtils.base64_to_str(SysUtils.get_env_var('SCRIPT'))
            FileUtils.create_file_with_content(script_path, script_content)
            get_logger().info("Script file created in '%s'", script_path)
        elif FileUtils.is_file(self._OSCAR_SCRIPT_PATH):
            script_path = self._OSCAR_SCRIPT_PATH
            get_logger().info("Script file found in '%s'", script_path)
        return script_path

    def execute_function(self):
        script_path = self._get_script_path()
        if script_path:
            try:
                pyinstaller_library_path = SysUtils.get_env_var('LD_LIBRARY_PATH')
                orig_library_path = SysUtils.get_env_var('LD_LIBRARY_PATH_ORIG')
                if orig_library_path:
                    SysUtils.set_env_var('LD_LIBRARY_PATH', orig_library_path)
                else:
                    SysUtils.delete_env_var('LD_LIBRARY_PATH')
                proc = subprocess.Popen(['/bin/sh', script_path],
                                                stdout=subprocess.PIPE,
                                                stderr=subprocess.STDOUT,
                                                encoding='utf-8',
                                                errors='ignore')
                SysUtils.set_env_var('LD_LIBRARY_PATH', pyinstaller_library_path)
                get_logger().debug("CONTAINER OUTPUT:\n %s", self.output)
                for line in proc.stdout:
                    get_logger().debug(line.strip())
                    self.output = self.output + line
            except subprocess.CalledProcessError as cpe:
                # Exit with user script return code if an
                # error occurs (Kubernetes handles the error)
                get_logger().error(cpe.output.decode(encoding='utf-8', errors='ignore'))
                sys.exit(cpe.returncode)
        else:
            get_logger().error('No user script found!')

    def create_response(self):
        return self.output

    def create_error_response(self):
        pass
