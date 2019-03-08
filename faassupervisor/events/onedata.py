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
{'Records': [{"id": "0000034500046EE9C6775...",
              "file": "file.txt",
              "path": "/my-onedata-space/files/file.txt",
              "eventSource": "OneTrigger",
              "eventTime": "2019-02-07T09:51:04.347823"}]}
'''
class OnedataEvent():
    
    def __init__(self, event_info):
        self.event = event_info['Records'][0]
        self._set_event_params()
        
    def _set_event_params(self):
        self.bucket_name = self.event['path'].split('/')[1]
        self.object_key = self.event['path']
        self.file_name = self.event['file']