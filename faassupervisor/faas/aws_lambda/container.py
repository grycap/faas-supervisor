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
"""In this module are defined the classes and methods
used to manage the container runtime jobs from the lambda environment."""

import subprocess
import os.path
import os

from faassupervisor.utils import FileUtils, SysUtils
from faassupervisor.logger import get_logger
from faassupervisor.exceptions import ContainerTimeoutExpiredWarning


class Container():
    """Used for defining Container jobs in the Lambda container runtime."""

    _CONTAINER_OUTPUT_FILE = SysUtils.join_paths(FileUtils.get_tmp_dir(), "container-stdout")

    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        self.script = None

        if hasattr(self.lambda_instance, 'script_path'):
            self.script = self.lambda_instance.script_path
        # Script to be executed every time (if defined)
        elif hasattr(self.lambda_instance, 'init_script_path'):
            self.script = self.lambda_instance.init_script_path
        else:
            raise Exception("Init: Script not defined.")
        if not os.path.isfile(self.script):
            raise Exception("Init: Script %s does not exist." % self.script)

    def invoke_function(self):
        if self.script:
            remaining_seconds = self.lambda_instance.get_remaining_time_in_seconds()
            get_logger().debug("Executing command: %s" % self.script)

            new_env = os.environ.copy()
            new_env.update(SysUtils.get_cont_env_vars())
            # Remove the library path set by Pyinstaller
            if 'LD_LIBRARY_PATH_ORIG' in new_env:
                new_env['LD_LIBRARY_PATH'] = new_env['LD_LIBRARY_PATH']
            elif 'LD_LIBRARY_PATH' in new_env:
                del new_env['LD_LIBRARY_PATH']

            with open(self._CONTAINER_OUTPUT_FILE, "wb") as out:
                with subprocess.Popen(['/bin/sh', self.script],
                                      stderr=subprocess.STDOUT,
                                      stdout=out,
                                      env=new_env,
                                      start_new_session=True) as process:
                    try:
                        rc = process.wait(timeout=remaining_seconds)
                        if rc != 0:
                            get_logger().warning("User script exited with code %s!" % rc)
                    except subprocess.TimeoutExpired:
                        get_logger().info("Stopping process '%s'", process)
                        process.kill()
                        raise ContainerTimeoutExpiredWarning()

            output = b''
            if FileUtils.is_file(self._CONTAINER_OUTPUT_FILE):
                output = FileUtils.read_file(self._CONTAINER_OUTPUT_FILE, file_mode="rb")
            return output
        else:
            return b''
