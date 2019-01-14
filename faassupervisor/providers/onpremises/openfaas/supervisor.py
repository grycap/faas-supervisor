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

import subprocess
import faassupervisor.utils as utils
from faassupervisor.interfaces.supervisor import SupervisorInterface
from faassupervisor.providers.onpremises.storage.minio import Minio

logger = utils.get_logger()
logger.info('SUPERVISOR: Initializing Openfaas supervisor')

class OpenfaasSupervisor(SupervisorInterface):
    
    output_folder = utils.join_paths(utils.get_random_tmp_folder(), "output")
    
    def __init__(self, **kwargs):
        self.event = kwargs['event']
        utils.create_folder(self.output_folder)
        utils.set_environment_variable('SCAR_OUTPUT_FOLDER', self.output_folder)

    @utils.lazy_property
    def storage_client(self):
        if Minio.is_minio_event(self.event):
            storage_client = Minio(self.output_folder, self.event)
        return storage_client
       
    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################
    
    def parse_input(self):
        utils.set_environment_variable('SCAR_INPUT_FILE', self.storage_client.download_input())
        print('SCAR_INPUT_FILE: {0}'.format(utils.get_environment_variable('SCAR_INPUT_FILE')))
    
    def parse_output(self):
        self.storage_client.upload_output()
    
    def execute_function(self):
        if utils.is_variable_in_environment('sprocess'):
            print("Executing user_script.sh")
            print(subprocess.call(['/bin/sh', utils.get_environment_variable('sprocess')], stderr=subprocess.STDOUT))
    
    def create_response(self):
        pass
    
    def create_error_response(self, message, status_code):
        pass

