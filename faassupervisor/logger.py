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

import logging
from faassupervisor.utils import is_variable_in_environment, get_environment_variable

def configure_logger():
    logger = logging.getLogger('supervisor')
    # Avoid initializing the logger several times
    if not logger.handlers:
        # Set logger configuration
        loglevel = logging.INFO
        if is_variable_in_environment("LOG_LEVEL"):
            loglevel = logging.getLevelName(get_environment_variable("LOG_LEVEL"))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        ch = logging.StreamHandler()
        ch.setFormatter(formatter)
        
        logger.setLevel(loglevel)
        logger.propagate = 0
        logger.addHandler(ch)
    
def get_logger():
    return logging.getLogger('supervisor')
