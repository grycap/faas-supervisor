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
""" Module used to parse the event received by the supervisor.

Event identification flow:

      Is a json
       event?  ------(no)---> Store event info as file
         |
       (yes)
         |
         V
    Is APIGateway? --(yes)--> Read Body, Parameters
         |                             |
        (no)                         (then)
         |-----------------------------|
         V
     Read Event.
     Storage keys? --(yes)-->  Read keys (minio, s3, onedata)
         |
        (no)
         |
         V
    Store event
    info as file
"""

import json
from faassupervisor.events.apigateway import ApiGatewayEvent
from faassupervisor.events.minio import MinioEvent
from faassupervisor.events.onedata import OnedataEvent
from faassupervisor.events.s3 import S3Event
from faassupervisor.events.unknown import UnknownEvent
from faassupervisor.logger import get_logger


def _is_api_gateway_event(event_info):
    return 'httpMethod' in event_info


def _has_storage_info(event_info):
    return 'Records' in event_info \
           and event_info['Records'] \
           and 'eventSource' in event_info['Records'][0]


class EventProvider():
    """ Receives an event a creates the appropriate class. """

    # pylint: disable=too-few-public-methods

    _EVENT_TYPE = {'ApiGatewayEvent' : 'APIGATEWAY',
                   'MinioEvent' : 'MINIO',
                   'OnedataEvent' : 'ONEDATA',
                   'S3Event' : 'S3',
                   'UnknownEvent' : 'UNKNOWN'}

    _S3_EVENT = "aws:s3"
    _MINIO_EVENT = "minio:s3"
    _ONEDATA_EVENT = "OneTrigger"

    def __init__(self, event, tmp_dir_path):
        self.tmp_dir_path = tmp_dir_path
        get_logger().info("Received event: %s", event)
        self._parse_event(event)

    def _parse_event(self, event):
        try:
            event_info = event if isinstance(event, dict) else json.loads(event)
            # Applies the event identification flow
            if _is_api_gateway_event(event_info):
                self.event = ApiGatewayEvent(event_info, self.tmp_dir_path)
                # Update event info with API request event body
                # to be further processed (if needed)
                event_info = self.event.event_info
            if _has_storage_info(event_info):
                self.event = self._get_storage_event(event_info)
            if not hasattr(self, 'event'):
                self.event = UnknownEvent(event_info, self.tmp_dir_path)

        except ValueError as err:
            # If the JSON loads fails, save the event anyways
            get_logger().exception(err)
            self.event = UnknownEvent(event, self.tmp_dir_path)

    def _get_storage_event(self, event_info):
        get_logger().info("Analyzing storage event")
        if self._is_s3_event(event_info):
            event = S3Event(event_info)
            get_logger().info("S3 event created")
        elif self._is_minio_event(event_info):
            event = MinioEvent(event_info)
            get_logger().info("Minio event created")
        elif self._is_onedata_event(event_info):
            event = OnedataEvent(event_info)
            get_logger().info("Onedata event created")
        else:
            event = UnknownEvent(event_info, self.tmp_dir_path)
        return event

    def _is_s3_event(self, event_info):
        return event_info['Records'][0]['eventSource'] == self._S3_EVENT

    def _is_minio_event(self, event_info):
        return event_info['Records'][0]['eventSource'] == self._MINIO_EVENT

    def _is_onedata_event(self, event_info):
        return event_info['Records'][0]['eventSource'] == self._ONEDATA_EVENT

    def get_event_type(self):
        """ Returns the event identifier based on the event class name. """
        return self._EVENT_TYPE.get(type(self.event).__name__)
