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

from faassupervisor.utils import FileUtils, SysUtils, ConfigUtils
from faassupervisor.logger import get_logger
from faassupervisor.exceptions import ContainerTimeoutExpiredWarning, FaasSupervisorError

class Container():
    """Used for defining Container jobs in the Lambda container runtime."""

    _CONTAINER_OUTPUT_FILE = SysUtils.join_paths(FileUtils.get_tmp_dir(), "container-stdout")

    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance

    def invoke_function(self):
        script = "%s/%s" % (self.lambda_instance.PERMANENT_FOLDER, ConfigUtils.read_cfg_var('init_script'))
        if script:
            remaining_seconds = self.lambda_instance.get_remaining_time_in_seconds()
            cmd = "/bin/sh %s %s %s" % (script,
                                        self.lambda_instance.input_folder,
                                        self.lambda_instance.output_folder)
            get_logger().debug("Executing command: %s" % cmd)

            with open(self._CONTAINER_OUTPUT_FILE, "wb") as out:
                with subprocess.Popen(cmd,
                                    stderr=subprocess.STDOUT,
                                    stdout=out,
                                    start_new_session=True) as process:
                    try:
                        process.wait(timeout=remaining_seconds)
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
