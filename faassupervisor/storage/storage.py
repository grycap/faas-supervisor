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
import abc
import importlib

class DefaultStorageProvider(metaclass=abc.ABCMeta):
    ''' All the different data providers must inherit from this class
    to ensure that the commands are defined consistently'''
        
    @abc.abstractmethod
    def download_input(self, event):
        pass

    @abc.abstractmethod    
    def upload_output(self, *args, **kwargs):
        pass

class StorageProvider(DefaultStorageProvider):

    _storage_type = {'MINIO': {'module' : 'faassupervisor.storage.providers.minio',
                              'class_name' : 'Minio'},
                    'ONEDATA': {'module' : 'faassupervisor.storage.providers.onedata',
                                'class_name' : 'Onedata'},
                    'S3': {'module' : 'faassupervisor.storage.providers.s3',
                           'class_name' : 'S3'},
                    }

    def __init__(self, storage_auth, storage_path):
        '''
        Receives StorageAuth class and a string with the storage path.
        Dynamically loads the module and the storage provider class needed.
        '''
        module = importlib.import_module(self._storage_type[storage_auth.type]['module'])
        class_ = getattr(module, self._storage_type[storage_auth.type]['class_name'])
        kwargs = {'Auth' : storage_auth, 'Path' : storage_path}
        self.storage_provider = class_(**kwargs)
        self.type = storage_auth.type

    def download_input(self, event, input_dir_path):
        '''
        Receives the event where the file information is and
        the tmp_dir_path where to store the downloaded file.
        
        Returns the file path where the file is downloaded
        '''
        return self.storage_provider.download_input(event, input_dir_path)

    def upload_output(self, output_dir_path):
        self.storage_provider.upload_output(output_dir_path)
    