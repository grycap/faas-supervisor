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

from faassupervisor.faas.aws.batch.batch import Batch
from faassupervisor.faas.aws.lambda_.function import LambdaInstance
from faassupervisor.faas.aws.lambda_.udocker import Udocker
from faassupervisor.supervisor import SupervisorInterface
import faassupervisor.logger as logger
import faassupervisor.utils as utils
import json
import subprocess
import traceback

class LambdaSupervisor(SupervisorInterface):
    
    @utils.lazy_property
    def batch(self):
        batch = Batch(self.lambda_instance)
        return batch
    
    @utils.lazy_property
    def udocker(self):
        udocker = Udocker(self.lambda_instance)
        return udocker
    
    def __init__(self, **kwargs):
        logger.get_logger().info('SUPERVISOR: Initializing AWS Lambda supervisor')
        self.lambda_instance = LambdaInstance(kwargs['event'], kwargs['context'])
        self.body = {}

    def _is_batch_execution(self):
        return utils.get_environment_variable("EXECUTION_MODE") == "batch"
    
    def _execute_batch(self):
        batch_ri = self.batch.invoke_batch_function()
        batch_logs = "Check batch logs with: \n  scar log -n {0} -ri {1}".format(self.lambda_instance.function_name, batch_ri)
        self.body["udocker_output"] = "Job delegated to batch.\n{0}".format(batch_logs)
        
    def _execute_udocker(self):
        try:
            udocker_output = self.udocker.launch_udocker_container()
            logger.get_logger().info("CONTAINER OUTPUT:\n {}".format(udocker_output))
            self.body["udocker_output"] = udocker_output            
        except subprocess.TimeoutExpired:
            logger.get_logger().warning("Container execution timed out")
            if(utils.get_environment_variable("EXECUTION_MODE") == "lambda-batch"):
                self._execute_batch()
    
    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################
    
    def execute_function(self):
        if self._is_batch_execution():
            self._execute_batch()
        else:
            self.udocker.prepare_container()
            self._execute_udocker()
            
    def create_error_response(self):
        exception_msg = traceback.format_exc()
        logger.get_logger().error("Exception launched:\n {0}".format(exception_msg))
        return {"statusCode" : 500, 
                "headers" : { 
                    "amz-lambda-request-id": self.lambda_instance.request_id, 
                    "amz-log-group-name": self.lambda_instance.log_group_name, 
                    "amz-log-stream-name": self.lambda_instance.log_stream_name },
                "body" : json.dumps({"exception" : exception_msg}),
                "isBase64Encoded" : False                
                }

    def create_response(self):
        return {"statusCode" : 200, 
                "headers" : { 
                    "amz-lambda-request-id": self.lambda_instance.request_id, 
                    "amz-log-group-name": self.lambda_instance.log_group_name, 
                    "amz-log-stream-name": self.lambda_instance.log_stream_name },
                "body" : json.dumps(self.body),
                "isBase64Encoded" : False }             
