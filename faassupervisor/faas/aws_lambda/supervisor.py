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
related with the AWS Lambda supervisor."""

import subprocess
import traceback
from faassupervisor.faas.aws_lambda.batch import Batch
from faassupervisor.faas.aws_lambda.function import LambdaInstance
from faassupervisor.faas.aws_lambda.udocker import Udocker
from faassupervisor.faas import DefaultSupervisor
from faassupervisor.logger import get_logger
from faassupervisor.utils import ConfigUtils, StrUtils
from faassupervisor.exceptions import NoLambdaContextError, \
    ContainerTimeoutExpiredWarning


def is_batch_execution():
    return ConfigUtils.read_cfg_var("execution_mode") == "batch"


def _is_lambda_batch_execution():
    return ConfigUtils.read_cfg_var("execution_mode") == "lambda-batch"


class LambdaSupervisor(DefaultSupervisor):
    """Supervisor class used in the Lambda environment."""

    def __init__(self, event, context):
        if context:
            get_logger().info('SUPERVISOR: Initializing AWS Lambda supervisor')
            self.lambda_instance = LambdaInstance(event, context)
            self.body = {}
        else:
            raise NoLambdaContextError()

    def _execute_batch(self):
        batch_ri = Batch(self.lambda_instance).invoke_batch_function()
        batch_logs = (f"Job delegated to batch.\n"
                      f"Check batch logs with:\n"
                      f"  scar log -n {self.lambda_instance.get_function_name()} -ri {batch_ri}")
        get_logger().info(batch_logs)
        self.body["udocker_output"] = batch_logs.encode('utf-8')

    def _execute_udocker(self):
        try:
            udocker = Udocker(self.lambda_instance)
            udocker.prepare_container()
            self.body["udocker_output"] = udocker.launch_udocker_container()
            get_logger().debug("CONTAINER OUTPUT:\n %s", self.body["udocker_output"].decode(encoding='utf-8', errors='ignore'))
        except (subprocess.TimeoutExpired, ContainerTimeoutExpiredWarning):
            get_logger().warning("Container execution timed out")
            if _is_lambda_batch_execution():
                self._execute_batch()

    def execute_function(self):
        if is_batch_execution():
            self._execute_batch()
        else:
            self._execute_udocker()

    def create_error_response(self):
        exception_msg = traceback.format_exc()
        get_logger().error("Exception launched:\n %s", exception_msg)
        return {
            "statusCode": 500,
            "headers": {
                "amz-lambda-request-id": self.lambda_instance.get_request_id(),
                "amz-log-group-name": self.lambda_instance.get_log_group_name(),
                "amz-log-stream-name": self.lambda_instance.get_log_stream_name()
            },
            "body": StrUtils.dict_to_base64str({"exception": exception_msg}),
            "isBase64Encoded": True,
        }

    def create_response(self):
        return {
            "statusCode": 200,
            "headers": {
                "amz-lambda-request-id": self.lambda_instance.get_request_id(),
                "amz-log-group-name": self.lambda_instance.get_log_group_name(),
                "amz-log-stream-name": self.lambda_instance.get_log_stream_name()
            },
            "body": StrUtils.bytes_to_base64str(self.body["udocker_output"]),
            "isBase64Encoded": True,
        }
