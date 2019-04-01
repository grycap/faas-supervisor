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
S3 event exaple:
{'Records': [{'awsRegion': 'us-east-1',
              'eventName': 'ObjectCreated:Put',
              'eventSource': 'aws:s3',
              'eventTime': '2019-02-23T11:40:46.473Z',
              'eventVersion': '2.1',
              'requestParameters': {'sourceIPAddress': '84.123.4.23'},
              'responseElements': {'x-amz-id-2': 'XXXXX',
                                   'x-amz-request-id': 'XXXXX'},
              's3': {'bucket': {'arn': 'arn:aws:s3:::scar-darknet-bucket',
                                'name': 'scar-darknet-bucket',
                                'ownerIdentity': {'principalId': 'XXXXX'}},
                     'configurationId': 'XXXXX',
                     'object': {'eTag': 'XXXXX',
                                'key': 'scar-darknet-s3/input/dog.jpg',
                                'sequencer': 'XXXXX',
                                'size': 999},
                     's3SchemaVersion': '1.0'},
              'userIdentity': {'principalId': 'AWS:XXXXX'}}]}

'''
from urllib.parse import unquote_plus
import faassupervisor.logger as logger

class S3Event():
    
    def __init__(self, event_info):
        self.event = event_info
        self.event_records = event_info['Records'][0]
        self._set_event_params()
        logger.get_logger().info("S3 event created")
        
    def _set_event_params(self):
        self.bucket_arn = self.event_records['s3']['bucket']['arn']
        self.bucket_name = self.event_records['s3']['bucket']['name']
        self.object_key = unquote_plus(self.event_records['s3']['object']['key'])
        self.file_name = self.object_key.split('/')[-1]
        
        
        