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

import boto3
import json
import faassupervisor.utils as utils
import faassupervisor.logger as logger

class Batch():
    '''
    Used for defining Batch jobs in the Batch environment
    '''
    
    @utils.lazy_property
    def client(self):
        client = boto3.client('batch')
        return client    
    
    def __init__(self, lambda_instance):
        self.lambda_instance = lambda_instance
        self.scar_batch_io_image_id = utils.get_environment_variable('BATCH_SUPERVISOR_IMG')
        self.script = self.get_user_script()
        self.input_file_path = utils.get_environment_variable("INPUT_FILE_PATH") if utils.is_variable_in_environment("INPUT_FILE_PATH") else ""
        self.container_environment_variables = []
        self.create_context()
    
    def create_context(self):
        self.context = {'function_name': self.lambda_instance.context.function_name,
                        'memory_limit_in_mb': self.lambda_instance.context.memory_limit_in_mb,
                        'aws_request_id': self.lambda_instance.context.aws_request_id,
                        'log_group_name': self.lambda_instance.context.log_group_name,
                        'log_stream_name': self.lambda_instance.context.log_stream_name}
    
    def set_container_variables(self, step):
        self.add_environment_variable("STEP", step)
        self.add_environment_variable("SCRIPT", self.script)
        self.add_environment_variable("AWS_LAMBDA_FUNCTION_NAME", self.lambda_instance.function_name)
        self.add_environment_variable("AWS_LAMBDA_EVENT", json.dumps(self.lambda_instance.raw_event))
        self.add_environment_variable("AWS_LAMBDA_CONTEXT", json.dumps(self.context))
        self.add_environment_variable("TMP_INPUT_DIR", self.lambda_instance.input_folder)
        self.add_environment_variable("TMP_OUTPUT_DIR",self.lambda_instance.output_folder)
        self.add_environment_variable("AWS_LAMBDA_REQUEST_ID", self.lambda_instance.request_id)

        if self.input_file_path:
            self.add_environment_variable("INPUT_FILE_PATH", self.input_file_path)
        
        for key, val in utils.get_environment_variables().items():
            if key.startswith("STORAGE_"):
                self.add_environment_variable(key, val)
        
        for user_var, value in utils._get_user_defined_variables().items():
            self.add_environment_variable(user_var, value)
    
    def add_environment_variable(self, name, value):
        return self.container_environment_variables.append({"name" : name, "value" : value})    
    
    def get_register_job_definition_args(self, job_name, step):
        self.set_container_variables(step)
        job_def_args = {
            'jobDefinitionName': job_name,
            "type": "container",
            "containerProperties": {
                "image": self.scar_batch_io_image_id,
                "vcpus": 1,
                "memory": self.lambda_instance.memory,                       
                "command": ["scar-batch-io"],
                "volumes": [
                    {"host": {"sourcePath": self.lambda_instance.input_folder},
                     "name": "TMP_INPUT_DIR"},
                    {"host":{"sourcePath": self.lambda_instance.output_folder},
                     "name": "TMP_OUTPUT_DIR"},
                ],
                "environment" : self.container_environment_variables,                             
                'mountPoints': [
                    {"sourceVolume": "TMP_INPUT_DIR",
                     "containerPath": self.lambda_instance.input_folder},
                    {"sourceVolume": "TMP_OUTPUT_DIR",
                     "containerPath": self.lambda_instance.output_folder},
                ],
            },
        }
        if step == "MED":
            job_def_args["containerProperties"]["command"] = []
            job_def_args["containerProperties"]["image"] = utils.get_environment_variable("IMAGE_ID")
            if self.script:
                job_def_args["containerProperties"]["command"] = ["{0}/script.sh".format(self.lambda_instance.input_folder)]
        
        return job_def_args
    
    def register_job_definition(self, job_name, step):
        logger.get_logger().info("Registering new job definition with name '{}'".format(job_name))
        register_job_args = self.get_register_job_definition_args(job_name, step)
        self.client.register_job_definition(**register_job_args)

    def invoke_batch_function(self):
        # Register batch Jobs
        self.register_job_definition("{}-in".format(self.lambda_instance.function_name), "INIT")
        self.register_job_definition(self.lambda_instance.function_name, "MED")
        self.register_job_definition("{}-out".format(self.lambda_instance.function_name), "END")
        # Submit batch jobs
        job_id = self.submit_init_job()
        lambda_job_id = self.submit_lambda_job(job_id)
        self.submit_end_job(lambda_job_id)
        return lambda_job_id

    def get_user_script(self):
        script = ""
        if utils.is_variable_in_environment('INIT_SCRIPT_PATH'):
            file_content = utils.read_file(utils.get_environment_variable('INIT_SCRIPT_PATH'), file_mode='rb')
            script = utils.utf8_to_base64_string(file_content)
        if utils.is_value_in_dict('script', self.lambda_instance.raw_event):
            script = self.lambda_instance.raw_event['script']
        return script
    
    def get_job_env_vars(self, step):
        variables= []
        self.add_environment_variable("STEP", step)
        self.add_environment_variable("SCRIPT", self.get_user_script())
        self.add_environment_variable("AWS_LAMBDA_FUNCTION_NAME", self.lambda_instance.function_name)
        self.add_environment_variable("INPUT_FILE_PATH", self.input_file_path)
        self.add_environment_variable("TMP_INPUT_DIR", self.lambda_instance.input_folder)
        self.add_environment_variable("TMP_OUTPUT_DIR", self.lambda_instance.output_folder)
        self.add_environment_variable("AWS_LAMBDA_REQUEST_ID", self.lambda_instance.request_id)

        for key,val in utils.get_environment_variables().items():
            if key.startswith('STORAGE_'):
                self.add_environment_variable(key, val)
        
        for user_var, value in utils._get_user_defined_variables().items():
            variables.append({"name" : user_var, "value" : value})
        return variables
    
    def get_job_args(self, step, job_id=None):
        job_name =  self.lambda_instance.function_name
        if step == 'INIT':
            job_name = "{}-in".format(job_name)
        elif step == 'END':
            job_name = "{}-out".format(job_name)
        job_def = {"jobDefinition" : job_name,
                   "jobName" : job_name,
                   "jobQueue" : self.lambda_instance.function_name,
                   "containerOverrides" : {
                       "environment" : self.get_job_env_vars(step)
                    }
                  }
        if job_id:
            job_def['dependsOn'] = [{'jobId' : job_id, 'type' : 'SEQUENTIAL'}]
        return job_def    
    
    def submit_batch_job(self, job_args):
        return self.client.submit_job(**job_args)["jobId"]
    
    def submit_init_job(self):
        return self.submit_batch_job(self.get_job_args('INIT'))
    
    def submit_lambda_job(self, job_id):
        return self.submit_batch_job(self.get_job_args('MED', job_id))
    
    def submit_end_job(self, job_id):
        return self.submit_batch_job(self.get_job_args('END', job_id))
