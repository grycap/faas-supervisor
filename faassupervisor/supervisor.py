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

import sys
import json
import faassupervisor.utils as utils
import faassupervisor.exceptions as excp
from faassupervisor.providers.aws.lambda_.supervisor import LambdaSupervisor
from faassupervisor.providers.onpremises.openfaas.supervisor import OpenfaasSupervisor

logger = utils.get_logger()

class Supervisor():
    
    allowed_supervisor_types = ['LAMBDA', 'OPENFAAS']

    def __init__(self, typ, **kwargs):
        ''' The class names initialized must follow the naming pattern 'type.lower().capitalize() + Supervisor'.
        For example the class 'LambdaSupervisor' is: 'LAMBDA'.lower().capitalize() + 'Supervisor'.
        '''
        targetclass = "{0}{1}".format(typ.lower().capitalize(), 'Supervisor')
        self.supervisor =  globals()[targetclass](**kwargs)

    @excp.exception(logger)
    def parse_input(self):
        try:
            utils.set_environment_variable('SCAR_INPUT_FILE', self.supervisor.storage_client.download_input())
            logger.info('SCAR_INPUT_FILE: {0}'.format(utils.get_environment_variable('SCAR_INPUT_FILE')))
        except excp.NoStorageProviderDefinedWarning:
            pass

    @excp.exception(logger)
    def parse_output(self):
        try:        
            self.supervisor.storage_client.upload_output()
        except excp.NoStorageProviderDefinedWarning:
            pass

    def run(self):
        try:
            self.parse_input()
            self.supervisor.execute_function()
            self.parse_output()
        except Exception:
            return self.supervisor.create_error_response()
        logger.info('Creating response')
        return self.supervisor.create_response()
    
def get_supervisor_type():
    typ = utils.get_environment_variable("SUPERVISOR_TYPE")
    is_allowed_environment(typ)
    return typ

@excp.exception(logger)
def is_allowed_environment(typ):
    if typ not in Supervisor.allowed_supervisor_types:
        raise excp.InvalidSupervisorTypeError()

def parse_input_args():
    ''' Only accepts 2 arguments in the following order: event, context.
    More arguments will be ignored.
    '''
    kwargs = {}
    if len(sys.argv) == 1:
        logger.info('SUPERVISOR: No input data')
    if len(sys.argv) >= 2:
        logger.info('SUPERVISOR: Event data found')
        kwargs['event'] = json.loads(sys.argv[1])
    if len(sys.argv) >= 3:
        logger.info('SUPERVISOR: Context data found')
        kwargs['context'] = json.loads(sys.argv[2])
    return kwargs

def start_supervisor(**kwargs):
    typ = get_supervisor_type()
    supervisor = Supervisor(typ, **kwargs)
    supervisor.run()    

def python_main(**kwargs):
    ''' Called when running from a Python environment.
    Receives the input from the method arguments.
    '''
    start_supervisor(**kwargs);

def main():
    ''' Called when running as binary.
    Receives the input from stdin.
    '''
    kwargs = parse_input_args()        
    start_supervisor(**kwargs);
    
if __name__ == "__main__":
    main()
