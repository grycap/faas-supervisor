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
used to manage the batch jobs from the lambda environment."""

import json
import boto3
from faassupervisor.utils import ConfigUtils


class Batch():
    """Used for defining Batch jobs in the Batch environment."""

    # pylint: disable=too-few-public-methods

    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        self.script = self._get_user_script()
        self._create_context()
        self._set_job_variables()
        self._create_batch_client()

    def _create_context(self):
        self.context = {
            'function_name': self.lambda_instance.get_function_name(),
            'memory_limit_in_mb': self.lambda_instance.get_memory(),
            'aws_request_id': self.lambda_instance.get_request_id(),
            'log_group_name': self.lambda_instance.get_log_group_name(),
            'log_stream_name': self.lambda_instance.get_log_stream_name()
        }

    def _create_batch_client(self):
        self.client = boto3.client('batch')

    def _set_job_variables(self):
        self.batch_job_env_vars = []
        if self.script:
            self._add_batch_job_env_var("SCRIPT", self.script)
        self._add_batch_job_env_var("EVENT", json.dumps(self.lambda_instance.raw_event))
        self._add_batch_job_env_var("CONTEXT", json.dumps(self.context))
        self._add_batch_job_env_var("AWS_LAMBDA_REQUEST_ID", self.context.get("aws_request_id"))

    def _add_batch_job_env_var(self, name, value):
        if name and value:
            self.batch_job_env_vars.append({"name": name, "value": value})

    def _get_user_script(self):
        script = ''
        if 'script' in self.lambda_instance.raw_event:
            script = self.lambda_instance.raw_event['script']
        return script
    
    def _get_overrides(self):
        batch = ConfigUtils.read_cfg_var("batch")
        if batch.get("multi_node_parallel").get("enabled") == True:
            return {
                "nodeOverrides": {
                    "nodePropertyOverrides": [
                        {
                            "containerOverrides": {
                                "environment": self.batch_job_env_vars
                            },
                            "targetNodes": "0:"
                        }
                    ]
                }
            }
        else:
            return {
                "containerOverrides": {
                    "environment": self.batch_job_env_vars
                }
            }

    def _get_job_args(self):
        job_name = self.context.get("function_name")
        overrides = self._get_overrides()
        job_def = {
            "jobDefinition": job_name,
            "jobName": job_name,
            "jobQueue": job_name,          
        }
        return {**job_def, **overrides}

    def _submit_batch_job(self, job_args):
        return self.client.submit_job(**job_args)["jobId"]

    def invoke_batch_function(self):
        """Submit job from a lambda instance."""
        return self._submit_batch_job(self._get_job_args())
