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

class SupervisorInterface(metaclass=abc.ABCMeta):
    ''' All the different supervisors must inherit from this class
    to ensure that the commands are defined consistently'''

    @abc.abstractmethod
    def parse_input(self):
        pass

    @abc.abstractmethod    
    def execute_function(self):
        pass
    
    @abc.abstractmethod    
    def parse_output(self):
        pass
    
    @abc.abstractmethod    
    def create_response(self):
        pass
    
    @abc.abstractmethod    
    def create_error_response(self, message, status_code):
        pass
     