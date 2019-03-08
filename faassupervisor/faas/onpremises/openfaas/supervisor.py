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

from faassupervisor.supervisor import SupervisorInterface
import faassupervisor.logger as logger
import faassupervisor.utils as utils
import subprocess

class OpenfaasSupervisor(SupervisorInterface):
    
    def __init__(self, **kwargs):
        logger.info('SUPERVISOR: Initializing Openfaas supervisor')
        if utils.is_variable_in_environment('sprocess'):
            self.script = utils.get_environment_variable('sprocess')

    ##################################################################
    ## The methods below must be defined for the supervisor to work ##
    ##################################################################
    
    def execute_function(self):
        if hasattr(self, 'script'):
            logger.info("Executing user defined script: '{}'".format(self.script))
            logger.info(subprocess.call(['/bin/sh', self.script], stderr=subprocess.STDOUT))
        else:
            logger.error('No user script found!')    

    def create_response(self):
        pass
    
    def create_error_response(self):
        pass
