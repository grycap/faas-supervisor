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
from faassupervisor.exceptions import NoStorageProviderDefinedWarning
from faassupervisor.interfaces.supervisor import SupervisorInterface
from faassupervisor.providers.onpremises.storage.minio import Minio

logger = utils.get_logger()

class OpenfaasSupervisor(SupervisorInterface):
    
    def __init__(self, **kwargs):
        logger.info('SUPERVISOR: Initializing Openfaas supervisor')
        self.event = kwargs['event']
        self.output_folder = utils.create_tmp_dir()
        self.output_folder_path = utils.join_paths(self.output_folder.name, "output")
        utils.create_folder(self.output_folder_path)
        utils.set_environment_variable('SCAR_OUTPUT_FOLDER', self.output_folder_path)

    @utils.lazy_property
    def storage_client(self):
        if Minio.is_minio_event(self.event):
            storage_client = Minio(self.event, self.output_folder_path)
        else:
            raise NoStorageProviderDefinedWarning()
        return storage_client
       
    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################
    
    def execute_function(self):
        if utils.is_variable_in_environment('sprocess'):
            print("Executing user_script.sh")
            print(subprocess.call(['/bin/sh', utils.get_environment_variable('sprocess')], stderr=subprocess.STDOUT))    
    
    def parse_input(self):
        pass
    
    def parse_output(self):
        pass
    
    def create_response(self):
        pass
    
    def create_error_response(self):
        pass
