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
from faassupervisor.providers.aws.batch.supervisor import BatchSupervisor
from faassupervisor.providers.onpremises.openfaas.supervisor import OpenfaasSupervisor

logger = utils.get_logger()

class Supervisor():
    
    allowed_supervisor_types = ['LAMBDA', 'BATCH', 'OPENFAAS']

    def __init__(self, typ, **kwargs):
        ''' The class names initialized must follow the naming pattern 'type.lower().capitalize() + Supervisor'.
        For example the class 'LambdaSupervisor' is: 'LAMBDA'.lower().capitalize() + 'Supervisor'.
        '''
        targetclass = "{0}{1}".format(typ.lower().capitalize(), 'Supervisor')
        self.supervisor =  globals()[targetclass](**kwargs)

    def run(self):
        try:
            self.supervisor.parse_input()
            self.supervisor.execute_function()
            self.supervisor.parse_output()
        except Exception:
            return self.supervisor.create_error_response()
        logger.info('Creating response')
        return self.supervisor.create_response()
    
def _get_supervisor_type():
    typ = utils.get_environment_variable("SUPERVISOR_TYPE")
    _is_allowed_environment(typ)
    return typ

@excp.exception(logger)
def _is_allowed_environment(typ):
    if typ not in Supervisor.allowed_supervisor_types:
        raise excp.InvalidSupervisorTypeError(sup_typ=typ)

def _parse_input_args():
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

def _start_supervisor(**kwargs):
    typ = _get_supervisor_type()
    supervisor = Supervisor(typ, **kwargs)
    return supervisor.run()

def python_main(**kwargs):
    ''' Called when running from a Python environment.
    Receives the input from the method arguments.
    '''
    return _start_supervisor(**kwargs);

def main():
    ''' Called when running as binary.
    Receives the input from stdin.
    '''
    kwargs = _parse_input_args()        
    return _start_supervisor(**kwargs);
    
if __name__ == "__main__":
    main()
