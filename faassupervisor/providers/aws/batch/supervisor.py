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
import faassupervisor.exceptions as excp
from faassupervisor.interfaces.supervisor import SupervisorInterface
from faassupervisor.providers.aws.batch.job import BatchJob
from faassupervisor.providers.aws.storage.s3 import S3

logger = utils.get_logger()

class BatchSupervisor(SupervisorInterface):
    
    @utils.lazy_property
    def storage_client(self):
        if S3.is_s3_event(self.batch_job.event):
            storage_client = S3(self.batch_job)
        else:
            raise excp.NoStorageProviderDefinedWarning()
        return storage_client     
    
    def __init__(self, **kwargs):
        logger.info('SUPERVISOR: Initializing AWS Batch supervisor')
        logger.debug("EVENT: {}".format(kwargs['event']))
        logger.debug("CONTEXT: {}".format(kwargs['context']))
        self.batch_job = BatchJob(kwargs['event'], kwargs['context'])

    def create_user_script(self):
        if utils.is_variable_in_environment('SCRIPT'):
            script_path = utils.join_paths(self.batch_job.input_folder, 'script.sh')
            script_content = utils.base64_to_utf8_string(utils.get_environment_variable('SCRIPT'))
            utils.create_file_with_content(script_path, script_content)
            logger.info("Script file created in '{0}'".format(script_path))
            utils.set_file_execution_rights(script_path)
     
    def upload_to_bucket(self):
        bucket_name = None
        bucket_folder = None
    
        if self.batch_job.has_output_bucket():
            bucket_name = self.batch_job.output_bucket
            logger.info("OUTPUT BUCKET SET TO {0}".format(bucket_name))
    
            if self.batch_job.has_output_bucket_folder():
                bucket_folder = self.batch_job.output_bucket_folder
                logger.info("OUTPUT FOLDER SET TO {0}".format(bucket_folder))
    
        elif self.batch_job.has_input_bucket():
            bucket_name = self.batch_job.input_bucket
            logger.info("OUTPUT BUCKET SET TO {0}".format(bucket_name))
    
        if bucket_name:
            self.storage_client.upload_output(bucket_name, bucket_folder)

    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################

    # Not needed in BATCH execution
    def execute_function(self):
        pass    
    
    @excp.exception(logger)    
    def parse_input(self):
        step = utils.get_environment_variable("STEP")
        if step == "INIT":
            logger.info("INIT STEP")
            self.create_user_script()
            if utils.is_variable_in_environment('INPUT_BUCKET'):
                self.storage_client.download_input()  
    
    @excp.exception(logger)
    def parse_output(self):
        step = utils.get_environment_variable("STEP")
        if step == "END":
            logger.info("END STEP")
            self.upload_to_bucket()
        
    def create_response(self):
        pass
    
    def create_error_response(self):
        pass
