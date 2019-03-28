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

from faassupervisor.faas.aws.batch.job import BatchJob
from faassupervisor.supervisor import SupervisorInterface
import faassupervisor.logger as logger
import faassupervisor.utils as utils

class BatchSupervisor(SupervisorInterface):
    
    def __init__(self, **kwargs):
        logger.get_logger().info('SUPERVISOR: Initializing AWS Batch supervisor')
        logger.get_logger().debug("EVENT: {}".format(kwargs['event']))
        logger.get_logger().debug("CONTEXT: {}".format(kwargs['context']))
        self.batch_job = BatchJob(kwargs['event'], kwargs['context'])

    def create_user_script(self):
        if utils.is_variable_in_environment('SCRIPT'):
            script_path = utils.join_paths(self.batch_job.input_folder, 'script.sh')
            script_content = utils.base64_to_utf8_string(utils.get_environment_variable('SCRIPT'))
            utils.create_file_with_content(script_path, script_content)
            logger.get_logger().info("Script file created in '{0}'".format(script_path))
            utils.set_file_execution_rights(script_path)
     
    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################

    # Not needed in BATCH execution
    def execute_function(self):
        pass    
    
    def parse_input(self):
        self.create_user_script()
    
    def create_response(self):
        pass
    
    def create_error_response(self):
        pass
