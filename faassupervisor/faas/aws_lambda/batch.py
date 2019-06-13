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
from faassupervisor.utils import SysUtils, FileUtils, StrUtils
from faassupervisor.logger import get_logger


class Batch():
    """Used for defining Batch jobs in the Batch environment."""

    # pylint: disable=too-few-public-methods

    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        self.scar_batch_io_image_id = SysUtils.get_env_var('BATCH_SUPERVISOR_IMG')
        self.script = self._get_user_script()
        self.input_file_path = SysUtils.get_env_var("INPUT_FILE_PATH")
        self.batch_job_env_vars = []
        self._create_context()
        self._create_batch_client()

    def _create_context(self):
        self.context = {'function_name': self.lambda_instance.get_function_name(),
                        'memory_limit_in_mb': self.lambda_instance.get_memory(),
                        'aws_request_id': self.lambda_instance.get_request_id(),
                        'log_group_name': self.lambda_instance.get_log_group_name(),
                        'log_stream_name': self.lambda_instance.get_log_stream_name()}

    def _create_batch_client(self):
        self.client = boto3.client('batch')

    def _set_container_variables(self, step):
        self._add_batch_job_env_var("STEP", step)
        self._add_batch_job_env_var("SCRIPT", self.script)
        self._add_batch_job_env_var("AWS_LAMBDA_FUNCTION_NAME", self.context.get("function_name"))
        self._add_batch_job_env_var("AWS_LAMBDA_EVENT", json.dumps(self.lambda_instance.raw_event))
        self._add_batch_job_env_var("AWS_LAMBDA_CONTEXT", json.dumps(self.context))
        self._add_batch_job_env_var("AWS_LAMBDA_REQUEST_ID", self.context.get("aws_request_id"))
        self._add_batch_job_env_var("TMP_INPUT_DIR", SysUtils.get_env_var("TMP_INPUT_DIR"))
        self._add_batch_job_env_var("TMP_OUTPUT_DIR", SysUtils.get_env_var("TMP_OUTPUT_DIR"))
        self._add_batch_job_env_var("INPUT_FILE_PATH", SysUtils.get_env_var("INPUT_FILE_PATH"))

        self._add_storage_variables()
        self._add_container_variables()

    def _add_storage_variables(self):
        for key, val in SysUtils.get_all_env_vars().items():
            if key.startswith("STORAGE_"):
                self._add_batch_job_env_var(key, val)

    def _add_container_variables(self):
        for user_var, value in SysUtils.get_cont_env_vars().items():
            self._add_batch_job_env_var(user_var, value)

    def _add_batch_job_env_var(self, name, value):
        if name and value:
            self.batch_job_env_vars.append({"name" : name, "value" : value})

    def _get_register_job_definition_args(self, job_name, step):
        self._set_container_variables(step)
        job_def_args = {
            'jobDefinitionName': job_name,
            "type": "container",
            "containerProperties": {
                "image": self.scar_batch_io_image_id,
                "vcpus": 1,
                "memory": self.context.get("memory_limit_in_mb"),
                "command": ["scar-batch-io"],
                "volumes": [
                    {"host": {"sourcePath": SysUtils.get_env_var("TMP_INPUT_DIR")},
                     "name": "TMP_INPUT_DIR"},
                    {"host":{"sourcePath": SysUtils.get_env_var("TMP_OUTPUT_DIR")},
                     "name": "TMP_OUTPUT_DIR"},
                ],
                "environment" : self.batch_job_env_vars,
                'mountPoints': [
                    {"sourceVolume": "TMP_INPUT_DIR",
                     "containerPath": SysUtils.get_env_var("TMP_INPUT_DIR")},
                    {"sourceVolume": "TMP_OUTPUT_DIR",
                     "containerPath": SysUtils.get_env_var("TMP_OUTPUT_DIR")},
                ],
            },
        }
        if step == "MED":
            job_def_args["containerProperties"]["command"] = []
            job_def_args["containerProperties"]["image"] = SysUtils.get_env_var("IMAGE_ID")
            if self.script:
                job_def_args["containerProperties"]["command"] = \
                    [f"{SysUtils.get_env_var('TMP_INPUT_DIR')}/script.sh"]

        return job_def_args

    def _register_job_definition(self, job_name, step):
        get_logger().info("Registering new job definition with name '%s'", job_name)
        register_job_args = self._get_register_job_definition_args(job_name, step)
        self.client.register_job_definition(**register_job_args)

    def _get_user_script(self):
        script = ""
        if SysUtils.is_var_in_env('INIT_SCRIPT_PATH'):
            file_content = FileUtils.read_file(SysUtils.get_env_var('INIT_SCRIPT_PATH'),
                                               file_mode='rb')
            script = StrUtils.bytes_to_base64str(file_content)
        if 'script' in self.lambda_instance.raw_event:
            script = self.lambda_instance.raw_event['script']
        return script

    def _get_job_env_vars(self, step):
        variables = []
        self._add_batch_job_env_var("STEP", step)
        self._add_batch_job_env_var("SCRIPT", self._get_user_script())
        self._add_batch_job_env_var("AWS_LAMBDA_FUNCTION_NAME", self.context.get("function_name"))
        self._add_batch_job_env_var("INPUT_FILE_PATH", SysUtils.get_env_var("INPUT_FILE_PATH"))
        self._add_batch_job_env_var("TMP_INPUT_DIR", SysUtils.get_env_var("TMP_INPUT_DIR"))
        self._add_batch_job_env_var("TMP_OUTPUT_DIR", SysUtils.get_env_var("TMP_OUTPUT_DIR"))
        self._add_batch_job_env_var("AWS_LAMBDA_REQUEST_ID", self.context.get("aws_request_id"))

        for key, val in SysUtils.get_all_env_vars().items():
            if key.startswith('STORAGE_'):
                self._add_batch_job_env_var(key, val)

        for user_var, value in SysUtils.get_cont_env_vars().items():
            variables.append({"name" : user_var, "value" : value})
        return variables

    def _get_job_args(self, step, job_id=None):
        job_name = self.context.get("function_name")
        if step == 'INIT':
            job_name = f"{job_name}-in"
        elif step == 'END':
            job_name = f"{job_name}-out"
        job_def = {"jobDefinition" : job_name,
                   "jobName" : job_name,
                   "jobQueue" : self.context.get("function_name"),
                   "containerOverrides" : {
                       "environment" : self._get_job_env_vars(step)}
                  }
        if job_id:
            job_def['dependsOn'] = [{'jobId' : job_id, 'type' : 'SEQUENTIAL'}]
        return job_def

    def _submit_batch_job(self, job_args):
        return self.client.submit_job(**job_args)["jobId"]

    def _submit_init_job(self):
        return self._submit_batch_job(self._get_job_args('INIT'))

    def _submit_lambda_job(self, job_id):
        return self._submit_batch_job(self._get_job_args('MED', job_id))

    def _submit_end_job(self, job_id):
        return self._submit_batch_job(self._get_job_args('END', job_id))

    def invoke_batch_function(self):
        """Method that creates batch jobs and invokes them from a lambda instance."""
        # Register batch Jobs
        self._register_job_definition(f"{self.context.get('function_name')}-in", "INIT")
        self._register_job_definition(self.context.get("function_name"), "MED")
        self._register_job_definition(f"{self.context.get('function_name')}-out", "END")
        # Submit batch jobs
        job_id = self._submit_init_job()
        lambda_job_id = self._submit_lambda_job(job_id)
        self._submit_end_job(lambda_job_id)
        return lambda_job_id
