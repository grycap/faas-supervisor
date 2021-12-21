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

from faassupervisor.utils import SysUtils
from faassupervisor.logger import get_logger

class Container():
    """Used for defining Container jobs in the Lambda container runtime."""

    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        self._get_cmd()

    def _get_cmd(self):
        if hasattr(self.lambda_instance, 'script_path'):
            self.script = self.lambda_instance.script_path
        elif hasattr(self.lambda_instance, 'init_script_path'):
            self.script = self.lambda_instance.init_script_path
        else:
            get_logger().error("No user script specified.")
            self.script = ""

    def invoke_function(self):
        if self.script:
            input_path = SysUtils.get_env_var("INPUT_FILE_PATH")
            output_dir = SysUtils.get_env_var("TMP_OUTPUT_DIR")
            cmd = "%s %s %s" % (self.script, input_path, output_dir)
            get_logger().debug("Executing command: %s" % cmd)
            return SysUtils.execute_cmd_and_return_output(cmd)
        else:
            return ""
