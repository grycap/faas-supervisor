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

import faassupervisor.logger as logger
import faassupervisor.utils as utils

def save_unknown_json_event(self, event, tmp_dir_path):
    file_path = utils.join_paths(tmp_dir_path, "event.json")
    logger.info("Received unknown JSON event and saved it in path '{0}'".format(file_path))
    utils.create_file_with_content(file_path, event)
    return file_path

def _save_unknown_event(self, event, tmp_dir_path):
    file_path = utils.join_paths(tmp_dir_path, "event_file")
    logger.info("Received unknown event and saved it in path '{0}'".format(file_path))
    utils.create_file_with_content(file_path, event, mode='wb')
    return file_path

class UnknownEvent():
    '''
    Class to manage unknown events
    '''    
    def __init__(self, event, tmp_dir_path, is_json=False):
        if is_json:
            self.file_path = save_unknown_json_event(event, tmp_dir_path)
        else:
            self.file_path = _save_unknown_event(event, tmp_dir_path)
        utils.set_environment_variable("INPUT_FILE_PATH", self.file_path)
        

