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

'''
Onedata event
{"Key": "/my-onedata-space/files/file.txt",
 "Records": [{"objectKey": "file.txt",
              "objectId": "0000034500046EE9C6775...",
              "eventTime": "2019-02-07T09:51:04.347823",
              "eventSource": "OneTrigger"}]
}              
'''
import faassupervisor.logger as logger

class OnedataEvent():
    
    def __init__(self, event_info):
        self.event = event_info
        self.event_records = event_info['Records'][0]
        self.object_key = event_info['Key']
        self._set_event_params()
        logger.get_logger().info("Onedata event created")        
        
    def _set_event_params(self):
        self.file_name = self.event_records['objectKey']
                