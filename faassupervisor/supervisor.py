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

from faassupervisor.interfaces.supervisor import SupervisorInterface
from faassupervisor.providers.aws.lambda_.supervisor import LambdaSupervisor
from faassupervisor.providers.onpremises.openfaas.supervisor import OpenfaasSupervisor

class Supervisor(SupervisorInterface):

    allowed_types = ['lambda', 'openfaas']

    def __init__(self, typ, **kwargs):
        ''' The class names initialized must follow the naming pattern 'type.capitalize() + Supervisor'.
        For example the class 'LambdaSupervisor' is: 'lambda'.capitalize() + 'Supervisor'.
        '''        
        if typ not in self.allowed_types:
            raise
        targetclass = "{0}{1}".format(typ.capitalize(), 'Supervisor')
        self.supervisor =  globals()[targetclass](**kwargs)

    def parse_input(self):
        self.supervisor.parse_input()

    def execute_function(self):
        self.supervisor.execute_function()
    
    def parse_output(self):
        self.supervisor.parse_output()
        
    def create_response(self):
        return self.supervisor.create_response()
    
    def create_error_response(self, message, status_code):
        return self.supervisor.create_error_response(message, status_code)
