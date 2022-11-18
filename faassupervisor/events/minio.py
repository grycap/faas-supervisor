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
"""Minio event example:

{"Key": "images/nature-wallpaper-229.jpg",
 "Records": [{"s3": {"object": {"key": "nature-wallpaper-229.jpg",
                                "userMetadata": { "content-type": "image/jpeg"},
                                "eTag": "dd20b7e4b74467ff16ce2d901c054419",
                                "contentType": "image/jpeg",
                                "sequencer": "153C9A7A7A3FB6AE",
                                "versionId": "1",
                                "size": 1019645},
                     "s3SchemaVersion": "1.0",
                     "bucket": {"ownerIdentity": {"principalId": "minio"},
                                "name": "images",
                                "arn": "arn:aws:s3:::images"},
                     "configurationId": "Config"},
              "requestParameters": {"sourceIPAddress": "10.244.0.0:34852"},
              "responseElements": {"x-amz-request-id": "153C9A7A7A3FB6AE",
                                   "x-minio-origin-endpoint": "http://10.244.1.3:9000"},
              "source": {"userAgent": "",
                         "host": "",
                         "port": ""},
              "eventVersion": "2.0",
              "eventName": "s3:ObjectCreated:Put",
              "awsRegion": "",
              "eventTime": "2018-06-29T10:23:44Z",
              "eventSource": "minio:s3",
              "userIdentity": {"principalId": "minio"}}],
 "EventName": "s3:ObjectCreated:Put"}
"""

from urllib.parse import unquote_plus
from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.utils import FileUtils


class MinioEvent(UnknownEvent):
    """Class to parse the Minio event."""

    # pylint: disable=too-few-public-methods

    _TYPE = 'MINIO'

    def __init__(self, event, provider_id='default'):
        super().__init__(event.get('event') or event)
        self.provider_id = provider_id

    def _set_event_params(self):
        self.bucket_arn = self.event_records['s3']['bucket']['arn']
        self.bucket_name = self.event_records['s3']['bucket']['name']
        self.object_key = unquote_plus(self.event_records['s3']['object']['key'])
        self.file_name = FileUtils.get_file_name(self.object_key)
        self.event_time = self.event_records['eventTime']
