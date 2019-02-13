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
import faassupervisor.utils as utils
import faassupervisor.exceptions as excp
import importlib
import json
logger = utils.get_logger()

class Supervisor():
    
    supervisor_type = { 'LAMBDA': {'module' : 'faassupervisor.providers.aws.lambda_.supervisor',
                                   'class_name' : 'LambdaSupervisor'},
                        'BATCH': {'module' : 'faassupervisor.providers.aws.batch.supervisor',
                                  'class_name' : 'BatchSupervisor'},
                        'OPENFAAS': {'module' : 'faassupervisor.providers.onpremises.openfaas.supervisor',
                                     'class_name' : 'OpenfaasSupervisor'},
                        }

    def __init__(self, typ, **kwargs):
        '''Dynamically loads the module and the supervisor class needed'''
        module = importlib.import_module(self.supervisor_type[typ]['module'])
        class_ = getattr(module, self.supervisor_type[typ]['class_name'])
        self.supervisor = class_(**kwargs) 

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
    if typ not in Supervisor.supervisor_type:
        raise excp.InvalidSupervisorTypeError(sup_typ=typ)

def _get_stdin():
    buf = ""
    for line in sys.stdin:
        buf = buf + line
    return buf

def _start_supervisor(**kwargs):
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
    kwargs = {'event': json.loads(_get_stdin())}
    return _start_supervisor(**kwargs)
    
if __name__ == "__main__":
    main()
