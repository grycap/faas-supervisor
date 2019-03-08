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

from faassupervisor.events.apigateway import ApiGatewayEvent
from faassupervisor.events.minio import MinioEvent
from faassupervisor.events.onedata import OnedataEvent
from faassupervisor.events.s3 import S3Event
from faassupervisor.events.unknown import UnknownEvent, save_unknown_json_event
import json

'''
Event identification flow:

      Is a json
       event?  ------(no)---> Store event info as file
         |
       (yes)
         |
         V      
    Is APIGateway? --(yes)--> Read Body, Parameters
         |
        (no)
         |
         V
     Read Event.
     Storage keys? --(yes)-->  Read keys (minio, s3, onedata)
         |
        (no)
         |
         V
    Store event
    info as file
'''

class DefaultEvent():
    pass

class EventProvider():
    '''
    Receives an event a creates the appropriate class
    '''
    _event_type = {'ApiGatewayEvent' : 'APIGATEWAY',
                   'MinioEvent' : 'MINIO',
                   'OnedataEvent' : 'ONEDATA',
                   'S3Event' : 'S3',
                   'UnknownEvent' : 'UNKNOWN'}
    
    def __init__(self, event, tmp_dir_path):
        self.tmp_dir_path = tmp_dir_path
        try:
            event_info = json.loads(event)
            # Check if the event comes from ApiGateway
            if self._is_api_gateway_event(event_info):
                self.event = ApiGatewayEvent(event_info, self.tmp_dir_path)
            elif self._has_known_storage_keys(event_info):
                self.event = self._create_storage_event(event_info)
            else:
                self.event = UnknownEvent(event_info, self.input_tmp_dir.name, is_json=True)
            # In addition, we always save the JSON event
            save_unknown_json_event(event_info, self.tmp_dir_path)
        except Exception:
            self.event = UnknownEvent(event, self.tmp_dir_path)
            
        
    def _is_api_gateway_event(self, event_info):
        return 'httpMethod' in event_info

    def _has_known_storage_keys(self, event_info):
        return 'Records' in event_info and event_info['Records'] and \
               'eventSource' in event_info['Records'][0]['eventSource']
    
    def _create_storage_event(self, event_info):
        if self._is_s3_event(event_info):
            event = S3Event(event_info, self.tmp_dir_path)
        elif self._is_minio_event(event_info):
            event = MinioEvent(event_info, self.tmp_dir_path)
        elif self._is_onedata_event(event_info):
            event = OnedataEvent(event_info, self.tmp_dir_path)
        else:
            event = UnknownEvent(event_info, self.tmp_dir_path, is_json=True)
        return event
    
    def _is_s3_event(self, event_info):
        return event_info['Records'][0]['eventSource'] == 'aws:s3'
        
    def _is_minio_event(self, event_info):
        return event_info['Records'][0]['eventSource'] == 'minio:s3'
    
    def _is_onedata_event(self, event_info):
        return event_info['Records'][0]['eventSource'] == 'OneTrigger'        

    def get_event_type(self):
        return self._event_type[type(self.event).__name__]
