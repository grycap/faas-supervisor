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
from faassupervisor.exceptions import StorageTypeError
import faassupervisor.utils as utils

class StorageAuth():
    '''
    Different storage types (to date):
        - Minio
        - OneData
        - S3
    '''
    _storage_type = {'S3', 'MINIO', 'ONEDATA'}   
    
    def __init__(self, storage_id):
        self.id = storage_id
        self.type = ''
        self.data = {}
        
    def set_type(self, typ):
        if typ in self._storage_type:
            self.type = typ
        else:
            raise StorageTypeError(typ=typ)
        
    def set_auth_var(self, auth_var, val):
        '''
        Common auth keys: USER|PASS|TOKEN|SPACE|HOST
        '''
        if auth_var and val:
            self.data[auth_var] = val
    
    def get(self, val):
        return self.data[val]

class StorageAuthData():

    def __init__(self):
        self.auth_data = {}
        self._read_storage_providers_envs()
    
    def _read_storage_providers_envs(self):
        '''
        Reads the global variables to create the providers needed.
        Variable schema:  STORAGE_AUTH_$1_$2_$3
        $1: MINIO | S3 | ONEDATA
        $2: STORAGE_ID (Specified in the function definition file, is unique for each storage defined)
        $3: USER | PASS | TOKEN | SPACE | HOST
        
        e.g.: STORAGE_AUTH_MINIO_12345_USER
        '''
        env_vars = utils.get_environment_variables()
        for env_key, env_val in env_vars.items():
            if env_key.startswith("STORAGE_AUTH_"):
                self._read_storage_auth(env_key.split("_"), env_val)
    
    def _read_storage_auth(self, prov_key, val):
        '''
        Creates the classes needed to initialize the storage providers.
        The provider_id can be composed by several fields:
        Two different cases:
          - key1 = "STORAGE_AUTH_MINIO_123_456_USER"
            ['STORAGE', 'AUTH', 'MINIO', '123', '456', 'USER']
          - key2 = "STORAGE_AUTH_MINIO_123-456_USER"
            ['STORAGE', 'AUTH', 'MINIO', '123-456', 'USER']
        '''
        storage_prov_id = "_".join(prov_key[3:-1])
        if storage_prov_id not in self.auth_data:
            self.auth_data[storage_prov_id] =  StorageAuth(storage_prov_id)
            self.auth_data[storage_prov_id].set_type(prov_key[2])
        self.auth_data[storage_prov_id].set_auth_var(prov_key[-1], val)
