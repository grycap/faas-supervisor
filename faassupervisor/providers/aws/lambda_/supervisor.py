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

import json
import subprocess
import traceback
import faassupervisor.utils as utils
from faassupervisor.exceptions import NoStorageProviderDefinedWarning
from faassupervisor.interfaces.supervisor import SupervisorInterface
from faassupervisor.providers.aws.lambda_.function import Lambda
from faassupervisor.providers.aws.lambda_.udocker import Udocker
from faassupervisor.providers.aws.batch.batch import Batch
from faassupervisor.providers.aws.apigateway.apigateway import ApiGateway
from faassupervisor.providers.aws.storage.s3 import S3

logger = utils.get_logger()

class LambdaSupervisor(SupervisorInterface):
    
    @utils.lazy_property
    def batch(self):
        batch = Batch(self.lambda_instance, self.scar_input_file)
        return batch
    
    @utils.lazy_property
    def apigateway(self):
        apigateway = ApiGateway(self.lambda_instance)
        return apigateway
    
    @utils.lazy_property
    def udocker(self):
        udocker = Udocker(self.lambda_instance, self.scar_input_file)
        return udocker
    
    @utils.lazy_property
    def storage_client(self):
        if S3.is_s3_event(self.lambda_instance.event):
            storage_client = S3(self.lambda_instance)
        else:
            raise NoStorageProviderDefinedWarning()
        return storage_client 
    
    def __init__(self, **kwargs):
        logger.info('SUPERVISOR: Initializing AWS Lambda supervisor')
        logger.debug("EVENT: {}".format(kwargs['event']))
        logger.debug("CONTEXT: {}".format(kwargs['context']))
        self.lambda_instance = Lambda(kwargs['event'], kwargs['context'])
        self.create_temporal_folders()
        self.create_event_file()
        self.status_code = 200
        self.body = {}
        self.scar_input_file = None

    def is_apigateway_event(self):
        return 'httpMethod' in self.lambda_instance.event           
           
    def prepare_udocker(self):
        self.udocker.create_image()
        self.udocker.create_container()
        self.udocker.create_command()
        
    def execute_udocker(self):
        try:
            udocker_output = self.udocker.launch_udocker_container()
            logger.info("CONTAINER OUTPUT:\n " + udocker_output)
            self.body["udocker_output"] = udocker_output            
        except subprocess.TimeoutExpired:
            logger.warning("Container execution timed out")
            if(utils.get_environment_variable("EXECUTION_MODE") == "lambda-batch"):
                self.execute_batch()
                
    def has_input_bucket(self):
        return hasattr(self, "input_bucket") and self.input_bucket and self.input_bucket != "" 

    def upload_to_bucket(self):
        bucket_name = None
        bucket_folder = None
        
        if self.lambda_instance.has_output_bucket():
            bucket_name = self.lambda_instance.output_bucket
            logger.debug("OUTPUT BUCKET SET TO {0}".format(bucket_name))
            if self.lambda_instance.has_output_bucket_folder():
                bucket_folder = self.lambda_instance.output_bucket_folder
                logger.debug("OUTPUT FOLDER SET TO {0}".format(bucket_folder))
                
        elif self.lambda_instance.has_input_bucket():
            bucket_name = self.lambda_instance.input_bucket
            logger.debug("OUTPUT BUCKET SET TO {0}".format(bucket_name))
            
        if bucket_name:
            self.storage_client.upload_output(bucket_name, bucket_folder)

    def create_temporal_folders(self):
        utils.create_folder(self.lambda_instance.input_folder)
        utils.create_folder(self.lambda_instance.output_folder)
        utils.create_folder(utils.get_environment_variable("UDOCKER_DIR"))
    
    def create_event_file(self):
        utils.create_file_with_content("{0}/event.json".format(self.lambda_instance.temporal_folder_path), json.dumps(self.lambda_instance.event))        

    def execute_batch(self):
        batch_ri = self.batch.invoke_batch_function()
        batch_logs = "Check batch logs with: \n  scar log -n {0} -ri {1}".format(self.lambda_instance.function_name, batch_ri)
        self.body["udocker_output"] = "Job delegated to batch.\n{0}".format(batch_logs)         

    def is_batch_execution(self):
        return utils.get_environment_variable("EXECUTION_MODE") == "batch"
    
    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################
    
    def parse_input(self):
        if  S3.is_s3_event(self.lambda_instance.event):
            self.input_bucket = self.storage_client.input_bucket
            logger.debug("INPUT BUCKET SET TO {0}".format(self.input_bucket))
            if self.is_batch_execution():
                self.scar_input_file = self.storage_client.file_download_path
            else:
                self.scar_input_file = self.storage_client.download_input()
            logger.debug("INPUT FILE SET TO {0}".format(self.scar_input_file))
        elif self.is_apigateway_event():
            self.apigateway.save_request_parameters()
            self.scar_input_file = self.apigateway.save_post_body()
            logger.debug("INPUT FILE SET TO {0}".format(self.scar_input_file)) 
    
    def parse_output(self):
        self.upload_to_bucket()
    
    def execute_function(self):
        if self.is_batch_execution():
            self.execute_batch()
        else:
            self.prepare_udocker()
            self.execute_udocker()
            
    def create_error_response(self):
        exception_msg = traceback.format_exc()
        logger.error("Exception launched:\n {0}".format(exception_msg))
        return {"statusCode" : 500, 
                "headers" : { 
                    "amz-lambda-request-id": self.lambda_instance.request_id, 
                    "amz-log-group-name": self.lambda_instance.log_group_name, 
                    "amz-log-stream-name": self.lambda_instance.log_stream_name },
                "body" : json.dumps({"exception" : exception_msg}),
                "isBase64Encoded" : False                
                }

    def create_response(self):
        return {"statusCode" : self.status_code, 
                "headers" : { 
                    "amz-lambda-request-id": self.lambda_instance.request_id, 
                    "amz-log-group-name": self.lambda_instance.log_group_name, 
                    "amz-log-stream-name": self.lambda_instance.log_stream_name },
                "body" : json.dumps(self.body),
                "isBase64Encoded" : False }             
