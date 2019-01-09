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
from faassupervisor.supervisortemplate import SupervisorTemplate

logger = utils.get_logger()
logger.info('SUPERVISOR: Initializing Openfaas supervisor')

class OpenfaasSupervisor(SupervisorTemplate):
    
    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################
    
    def parse_input(self):
        pass
    
    def parse_output(self):
        pass
    
    def execute_function(self):
        pass
    
    def create_response(self):
        pass
    
    def create_error_response(self, message, status_code):
        pass
