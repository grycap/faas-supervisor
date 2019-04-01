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

class StoragePath():
    
    def __init__(self, storage_id, path):
        self.id = storage_id
        self.path = path
        
class StoragePathData():
    ''' All the different data providers must inherit from this class
    to ensure that the commands are defined consistently'''

    def __init__(self):
        self.input = {}
        self.output = {}
        self._read_storage_path_envs()
    
    def _read_storage_path_envs(self):
        '''
        Reads the global variables to create the providers needed.
        Variable schema:  STORAGE_PATH_$1_$2
        $1: INPUT | OUTPUT
        $2: STORAGE_ID (Specified in the function definition file, is unique for each storage defined)
        e.g.: STORAGE_PATH_INPUT_12345
        '''
        env_vars = utils.get_environment_variables()
        for env_key, env_val in env_vars.items():
            if env_key.startswith("STORAGE_PATH_"):
                self._read_storage_path(env_key.split("_"), env_val)
    
    def _read_storage_path(self, prov_key, val):
        '''
        Creates the classes needed to initialize the storage providers.
        The provider_id can be composed by several fields:
        Two different cases:
          - key1 = "STORAGE_PATH_INPUT_123_456"
            ['STORAGE', 'PATH', 'INPUT', '123', '456']
          - key2 = "STORAGE_PATH_INPUT_123-456"
            ['STORAGE', 'PATH', 'INPUT', '123-456']
        '''
        storage_id = "_".join(prov_key[3:])
        if prov_key[2] == 'INPUT':
            self.input[storage_id] =  StoragePath(storage_id, val)
        else:
            self.output[storage_id] =  StoragePath(storage_id, val)
    