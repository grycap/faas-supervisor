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

from faassupervisor.events.events import EventProvider
from faassupervisor.storage.auth import StorageAuthData
from faassupervisor.storage.path import StoragePathData
from faassupervisor.storage.storage import StorageProvider
import abc
import faassupervisor.exceptions as excp
import faassupervisor.logger as logger
import faassupervisor.utils as utils
import importlib

class SupervisorInterface(metaclass=abc.ABCMeta):
    ''' All the different supervisors must inherit from this class
    to ensure that the commands are defined consistently'''

    @abc.abstractmethod    
    def execute_function(self):
        pass
    
    @abc.abstractmethod    
    def create_response(self):
        pass
    
    @abc.abstractmethod    
    def create_error_response(self, message, status_code):
        pass

class Supervisor():
    
    supervisor_type = { 'LAMBDA': {'module' : 'faassupervisor.faas.aws.lambda_.supervisor',
                                   'class_name' : 'LambdaSupervisor'},
                        'BATCH': {'module' : 'faassupervisor.faas.aws.batch.supervisor',
                                  'class_name' : 'BatchSupervisor'},
                        'OPENFAAS': {'module' : 'faassupervisor.faas.openfaas.supervisor',
                                     'class_name' : 'OpenfaasSupervisor'},
                        }

    def __init__(self, typ, **kwargs):
        # Temporal directories where the input/output data will be stored
        # and deleted when the execution finishes
        self._create_tmp_dirs()
        # Parse the event data
        self.event = EventProvider(kwargs['event'], self._get_input_dir())
        self.input_data_providers = []
        self.output_data_providers = []
        # Create the supervisor
        # Dynamically loads the module and the supervisor class needed
        module = importlib.import_module(self.supervisor_type[typ]['module'])
        class_ = getattr(module, self.supervisor_type[typ]['class_name'])
        self.supervisor = class_(**kwargs)        
        
    def _create_tmp_dirs(self):
        if _is_batch_environment():
            if utils.get_environment_variable("STEP") == "INIT":
                utils.create_folder(utils.get_environment_variable("TMP_INPUT_DIR"))
                utils.create_folder(utils.get_environment_variable("TMP_OUTPUT_DIR"))
        else:
            # Temporal directory where the data will be stored
            # and deleted when the execution finishes
            self.input_tmp_dir = utils.create_tmp_dir()
            self.output_tmp_dir = utils.create_tmp_dir()
            utils.set_environment_variable("TMP_INPUT_DIR", self.input_tmp_dir.name)
            utils.set_environment_variable("TMP_OUTPUT_DIR", self.output_tmp_dir.name)
        
    def _get_input_dir(self):
        if _is_batch_environment():
            return utils.get_environment_variable("TMP_INPUT_DIR")
        return self.input_tmp_dir.name
    
    def _get_output_dir(self):
        if _is_batch_environment():
            return utils.get_environment_variable("TMP_OUTPUT_DIR")
        return self.output_tmp_dir.name
        
    def _create_storage_providers(self):
        logger.get_logger().info("Reading STORAGE_AUTH variables")
        storage_auths = StorageAuthData()
        logger.get_logger().info("Reading STORAGE_PATH variables")
        storage_paths = StoragePathData()
        # Create input data providers
        for storage_id, storage_path in storage_paths.input.items():
            self.input_data_providers.append(StorageProvider(storage_auths.auth_data[storage_id], storage_path))
            logger.get_logger().info("Found '{}' input provider".format(self.input_data_providers[-1].type))
        # Create output data providers
        for storage_id, storage_path in storage_paths.output.items():
            self.output_data_providers.append(StorageProvider(storage_auths.auth_data[storage_id], storage_path))
            logger.get_logger().info("Found '{}' output provider".format(self.input_data_providers[-1].type))
        
    @excp.exception(logger.get_logger())
    def _parse_input(self):
        '''
        Download input data from storage provider or 
        save data from POST request
        '''
        if _is_batch_environment():
            # Don't download anything if not INIT step
            if utils.get_environment_variable("STEP") != "INIT":
                return
            # Manage batch extra steps
            self.supervisor.parse_input()
        
        # event_type could be: 'APIGATEWAY'|'MINIO'|'ONEDATA'|'S3'|'UNKNOWN'
        event_type = self.event.get_event_type()
        logger.get_logger().info("Downloading input file from event type '{}'".format(event_type))
        if event_type != 'APIGATEWAY' and event_type != 'UNKNOWN':
            for data_provider in self.input_data_providers:
                # data_provider.type could be: 'MINIO'|'ONEDATA'|'S3'
                if data_provider.type == event_type:
                    input_file_path = data_provider.download_input(self.event, self._get_input_dir())
                    if input_file_path:
                        utils.set_environment_variable("INPUT_FILE_PATH", input_file_path)
                        logger.get_logger().info("INPUT_FILE_PATH variable set to '{}'".format(input_file_path))
                    break
    
    @excp.exception(logger.get_logger())
    def _parse_output(self):
        # Don't upload anything if not END step
        if _is_batch_environment() and utils.get_environment_variable("STEP") != "END":
            return
        
        for data_provider in self.output_data_providers:
            data_provider.upload_output(self._get_output_dir())
            
    @excp.exception(logger.get_logger())
    def run(self):
        try:
            self._create_storage_providers()
            self._parse_input()
            self.supervisor.execute_function()
            self._parse_output()
        except Exception as ex:
            logger.get_logger().exception(ex)
            logger.get_logger().error('Creating error response')
            return self.supervisor.create_error_response()
        logger.get_logger().info('Creating response')
        return self.supervisor.create_response()            

def _is_batch_environment():
    return _get_supervisor_type() == 'BATCH'    
    
def _get_supervisor_type():
    typ = utils.get_environment_variable("SUPERVISOR_TYPE")
    _is_allowed_environment(typ)
    return typ

@excp.exception(logger.get_logger())
def _is_allowed_environment(typ):
    if typ not in Supervisor.supervisor_type:
        raise excp.InvalidSupervisorTypeError(sup_typ=typ)

def _start_supervisor(**kwargs):
    logger.configure_logger()
    typ = _get_supervisor_type()
    supervisor = Supervisor(typ, **kwargs)
    return supervisor.run()

def python_main(**kwargs):
    ''' Called when running from a Python environment.
    Receives the input from the method arguments.
    '''
    return _start_supervisor(**kwargs)

def main():
    ''' Called when running as binary.
    Receives the input from stdin.
    '''
    kwargs = {'event': utils.get_stdin()}
    return _start_supervisor(**kwargs)
    
if __name__ == "__main__":
    main()
