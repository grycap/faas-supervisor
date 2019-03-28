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

# Executed inside the containers launched in each batch job
class BatchJob():
    
    def __init__(self, event, context):
        self.event = event
        self._set_context_info(context)
        self._set_tmp_folders()         
        
    def _set_context_info(self, context):
        self.request_id = context['aws_request_id']
        self.memory = int(context['memory_limit_in_mb'])
        self.function_name = context['function_name']
        self.log_group_name = context['log_group_name']
        self.log_stream_name = context['log_stream_name']        

    def _set_tmp_folders(self):
        self.input_folder = utils.get_environment_variable("TMP_INPUT_DIR")
        self.output_folder = utils.get_environment_variable("TMP_OUTPUT_DIR")       
