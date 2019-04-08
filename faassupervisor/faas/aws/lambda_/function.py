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

import faassupervisor.utils as utils
import json
from shutil import copyfile

class LambdaInstance():

    def __init__(self, event, context):
        self.raw_event = event
        self.context = context
        self.request_id = context.aws_request_id
        utils.set_environment_variable('AWS_LAMBDA_REQUEST_ID', context.aws_request_id)
        self.memory = int(context.memory_limit_in_mb)
        self.arn = context.invoked_function_arn
        self.function_name = context.function_name
        self.log_group_name = self.context.log_group_name
        self.log_stream_name = self.context.log_stream_name  
        self.permanent_folder = "/var/task"
        self._set_tmp_folders()
        
        # Check for script in function event
        if utils.is_value_in_dict('script', self.raw_event): 
            self.script_path = "{0}/script.sh".format(self.input_folder)
            script_content = utils.base64_to_utf8_string(self.raw_event['script'])
            utils.create_file_with_content(self.script_path, script_content)
        # Container with args
        elif utils.is_value_in_dict('cmd_args', self.raw_event):
            # Add args
            self.cmd_args = json.loads(self.raw_event['cmd_args'])
        # Script to be executed every time (if defined)
        elif utils.is_variable_in_environment('INIT_SCRIPT_PATH'):
            # Add init script
            self.init_script_path = "{0}/init_script.sh".format(self.input_folder)
            copyfile(utils.get_environment_variable("INIT_SCRIPT_PATH"), self.init_script_path)    

    def _set_tmp_folders(self):
        self.input_folder = utils.get_environment_variable("TMP_INPUT_DIR")
        self.output_folder = utils.get_environment_variable("TMP_OUTPUT_DIR")

    def get_invocation_remaining_seconds(self):
        return int(self.context.get_remaining_time_in_millis() / 1000) - int(utils.get_environment_variable('TIMEOUT_THRESHOLD'))
    
